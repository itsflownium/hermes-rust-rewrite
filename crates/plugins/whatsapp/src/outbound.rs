use crate::{common::MediaKind, intake::Source};
use hermes_gateway_core::messaging::{
    AttachmentRef, Control, DeliveryBody, DeliveryIntent, DeliveryPart, DeliveryResult, Effect,
    EffectDependency, EffectIdentity, EffectObservation, EffectOperation, GatewayError, Ingress,
    Message, PreparationFailure, Prompt, ProviderDeliveryState, ReceiptKind, Response, Room,
    SendAttempt, SpokenReplyMode, UploadStep,
    runtime::{blocking, db, now},
    storage, voice_codec,
};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use std::time::Duration;
use unicode_segmentation::UnicodeSegmentation;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_whatsapp_outgoing (
    connection TEXT NOT NULL, room TEXT NOT NULL, message TEXT NOT NULL,
    delivery TEXT NOT NULL, text TEXT NOT NULL,
    PRIMARY KEY(connection,room,message)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_prompts (
    connection TEXT NOT NULL, delivery TEXT NOT NULL, action TEXT NOT NULL, actor TEXT NOT NULL,
    room TEXT NOT NULL, message TEXT, expires INTEGER NOT NULL, options TEXT NOT NULL,
    PRIMARY KEY(connection,delivery)
);
";

const TEXT_LIMIT: usize = 3_800;
const MAX_PARTS: usize = 128;

fn part(intent: &DeliveryIntent, body: DeliveryBody, operation: EffectOperation) -> DeliveryPart {
    DeliveryPart {
        room: intent.room.clone(),
        body,
        effect: Effect {
            operation,
            ..Effect::default()
        },
        edit_target: None,
    }
}

pub(crate) fn plan(
    intent: &DeliveryIntent,
    ingress: Option<&Ingress>,
    cloud: bool,
    voice: bool,
) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    let mut plan = Vec::new();
    match &intent.body {
        DeliveryBody::Reply(message) => {
            let mut chunks = split(&format_text(&message.text));
            if voice
                && let Some(ingress) = ingress
                && let Some(text) = chunks.first()
                && voice_for(ingress, intent).map_err(|_| PreparationFailure::Rejected)?
            {
                plan.push(part(
                    intent,
                    DeliveryBody::Reply(Message::text(text)),
                    EffectOperation::Message,
                ));
                plan[0].effect.operation_key = Some(
                    db(storage::key("whatsapp-voice", &intent.id))
                        .map_err(|_| PreparationFailure::Rejected)?,
                );
            }
            for text in chunks.drain(..) {
                plan.push(part(
                    intent,
                    DeliveryBody::Reply(Message::text(text)),
                    EffectOperation::Message,
                ));
            }
            for artifact in &message.attachments {
                let media = DeliveryBody::Reply(Message {
                    text: String::new(),
                    attachments: vec![artifact.clone()],
                });
                let operation = if cloud {
                    let upload =
                        u32::try_from(plan.len()).map_err(|_| PreparationFailure::Rejected)?;
                    plan.push(part(
                        intent,
                        media.clone(),
                        EffectOperation::Upload {
                            artifact: artifact.clone(),
                            step: UploadStep::Single,
                        },
                    ));
                    EffectOperation::Publish {
                        using: vec![EffectDependency {
                            part: upload,
                            kind: ReceiptKind::File,
                        }],
                    }
                } else {
                    EffectOperation::Message
                };
                plan.push(part(intent, media, operation));
            }
        }
        DeliveryBody::Prompt { .. } => {
            plan.push(part(intent, intent.body.clone(), EffectOperation::Message))
        }
        DeliveryBody::Progress { .. } if !cloud => {
            plan.push(part(intent, intent.body.clone(), EffectOperation::Message))
        }
        _ => return Err(PreparationFailure::Unsupported),
    }
    if plan.is_empty() || plan.len() > MAX_PARTS {
        return Err(PreparationFailure::Rejected);
    }
    for (ordinal, part) in plan.iter_mut().enumerate() {
        if part.effect.operation_key.is_none()
            && matches!(
                part.effect.operation,
                EffectOperation::Message | EffectOperation::Publish { .. }
            )
        {
            part.effect.operation_key = Some(
                storage::key("whatsapp-effect", &(&intent.id, ordinal))
                    .map_err(|_| PreparationFailure::Rejected)?,
            );
        }
    }
    Ok(plan)
}

fn voice_for(ingress: &Ingress, intent: &DeliveryIntent) -> Result<bool, GatewayError> {
    let mode = ingress.spoken_reply_mode(&intent.room)?;
    if mode == SpokenReplyMode::Off {
        return Ok(false);
    }
    ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_deliveries d
             JOIN messaging_effect_sources s ON s.delivery=d.id
             LEFT JOIN messaging_whatsapp_voice v ON v.connection=d.connection AND v.event=s.event
             WHERE d.connection=?1 AND d.id=?2 AND d.sender IS NOT NULL AND (?3 OR v.event IS NOT NULL))",
            params![ingress.connection_name(),intent.id,mode == SpokenReplyMode::Always], |row| row.get(0),
        )?)
    })))
}

pub(crate) fn is_voice(attempt: &SendAttempt) -> bool {
    attempt
        .payload
        .effect
        .operation_key
        .as_deref()
        .is_some_and(|key| key.starts_with("whatsapp-voice-"))
}

pub(crate) fn delivery_actor(
    ingress: &Ingress,
    attempt: &SendAttempt,
) -> Result<Option<String>, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row(
            "SELECT json_extract(sender,'$.id') FROM messaging_deliveries WHERE connection=?1 AND id=?2",
            params![ingress.connection_name(),attempt.delivery_id], |row| row.get(0),
        )?)
    })))
}

pub(crate) async fn encode_voice(
    ingress: &Ingress,
    attempt: &SendAttempt,
) -> Result<Vec<u8>, GatewayError> {
    let artifact = ingress.synthesize_reply(attempt).await?;
    let wav = ingress.reply_artifact(attempt, &artifact).await?;
    blocking(move || Ok(voice_codec::encode_voice_note(&wav)?.bytes)).await
}

fn format_text(text: &str) -> String {
    let mut rendered = String::with_capacity(text.len());
    let mut code = false;
    for (index, section) in text.split('`').enumerate() {
        if index != 0 {
            rendered.push('`');
            code = !code;
        }
        if code {
            rendered.push_str(section);
        } else {
            rendered.push_str(
                &section
                    .replace("**", "*")
                    .replace("__", "_")
                    .replace("~~", "~"),
            );
        }
    }
    rendered
}

fn split(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut chunk = String::new();
    let mut units = 0;
    for glyph in text.graphemes(true) {
        let count = glyph.encode_utf16().count();
        if count > TEXT_LIMIT {
            for character in glyph.chars() {
                if units + character.len_utf16() > TEXT_LIMIT {
                    parts.push(std::mem::take(&mut chunk));
                    units = 0;
                }
                units += character.len_utf16();
                chunk.push(character);
            }
            continue;
        }
        if units + count > TEXT_LIMIT {
            parts.push(std::mem::take(&mut chunk));
            units = 0;
        }
        units += count;
        chunk.push_str(glyph);
    }
    if !chunk.trim().is_empty() {
        parts.push(chunk);
    }
    parts
}

#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub(crate) struct OptionChoice {
    pub id: String,
    pub label: String,
    response: Response,
}

pub(crate) struct PromptPlan {
    pub text: String,
    pub choices: Vec<OptionChoice>,
    pub clarification: bool,
}

impl PromptPlan {
    pub fn text_fallback(&self) -> String {
        let mut message = self.text.clone();
        for (index, choice) in self.choices.iter().enumerate() {
            message.push_str(&format!("\n{}. {}", index + 1, choice.label));
        }
        message.push_str("\nReply to this message with the number of your choice.");
        message
    }
}

pub(crate) fn prompt(
    ingress: &Ingress,
    attempt: &SendAttempt,
    action: &str,
    prompt: &Prompt,
) -> Result<PromptPlan, GatewayError> {
    let (text, options, clarification) = match prompt {
        Prompt::Approval {
            text,
            allow_session,
            allow_persistent,
        } => {
            let mut options = vec![("Approve once", Response::Approve)];
            if *allow_session {
                options.push(("Approve for session", Response::ApproveForSession));
            }
            if *allow_persistent {
                options.push(("Always approve", Response::ApprovePersistently));
            }
            options.push(("Deny", Response::Deny));
            (text, options, false)
        }
        Prompt::Clarification { question, choices } => (
            question,
            choices
                .iter()
                .take(10)
                .map(|label| (label.as_str(), Response::Clarify(label.clone())))
                .collect(),
            true,
        ),
    };
    let text = text.chars().take(940).collect();
    let choices = options
        .into_iter()
        .enumerate()
        .map(|(index, (label, response))| {
            let id = storage::key("wa-choice", &(&attempt.delivery_id, action, index))?;
            Ok(OptionChoice {
                id,
                label: label.chars().take(55).collect(),
                response,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|_| GatewayError::TemporarilyUnavailable)?;
    ingress.checked(true, |_| db(ingress.storage().write(|tx| {
        let connection = ingress.connection_name();
        let row = tx.query_row(
            "SELECT a.recipient,p.expires FROM messaging_actions a
             JOIN messaging_action_authority p USING(id)
             JOIN messaging_deliveries d ON d.connection=a.connection AND d.route=a.route
             WHERE d.id=?1 AND a.connection=?2 AND a.id=?3 AND a.resolved IS NULL AND p.expires>?4",
            params![attempt.delivery_id,connection,action,now()],
            |row| Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?)),
        ).optional()?;
        let Some((actor,expires)) = row else { return Ok(Err(GatewayError::Forbidden)) };
        tx.execute(
            "INSERT OR IGNORE INTO messaging_whatsapp_prompts(connection,delivery,action,actor,room,expires,options)
                VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![connection,attempt.delivery_id,action,actor,serde_json::to_string(&attempt.payload.room)?,
                expires,serde_json::to_string(&choices)?],
        )?;
        Ok(Ok(()))
    }))?)?;
    Ok(PromptPlan {
        text,
        choices,
        clarification,
    })
}

pub(crate) fn answer(ingress: &Ingress, source: &Source) -> Result<Option<Control>, GatewayError> {
    let Some(anchor) = &source.quote else {
        return Ok(None);
    };
    let saved = ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT action,options FROM messaging_whatsapp_prompts
             WHERE connection=?1 AND room=?2 AND message=?3 AND actor=?4 AND expires>?5",
                    params![
                        ingress.connection_name(),
                        serde_json::to_string(&source.room)?,
                        anchor,
                        source.actor,
                        now()
                    ],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            storage::decode::<Vec<OptionChoice>>(r, 1)?,
                        ))
                    },
                )
                .optional()?)
        }))
    })?;
    let Some((action_id, options)) = saved else {
        return Ok(None);
    };
    let response = if let Some(choice) = &source.choice {
        options
            .into_iter()
            .find(|option| option.id == *choice)
            .map(|v| v.response)
    } else {
        let text = source
            .text
            .strip_prefix("/answer ")
            .unwrap_or(&source.text)
            .trim();
        let index = text
            .parse::<usize>()
            .ok()
            .and_then(|index| index.checked_sub(1));
        if let Some(index) = index {
            options.get(index).map(|option| option.response.clone())
        } else if options
            .first()
            .is_some_and(|option| matches!(option.response, Response::Clarify(_)))
            && !text.is_empty()
            && text.len() <= 500
        {
            Some(Response::Clarify(text.into()))
        } else {
            None
        }
    };
    Ok(response.map(|response| Control::Respond {
        action_id,
        response,
    }))
}

pub(crate) fn is_prompt(
    ingress: &Ingress,
    room: &Room,
    anchor: &str,
) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_whatsapp_prompts WHERE connection=?1 AND room=?2 AND message=?3)",
            params![ingress.connection_name(),serde_json::to_string(room)?,anchor],
            |row| row.get(0),
        )?)
    })))
}

pub(crate) fn record(
    ingress: &Ingress,
    attempt: &SendAttempt,
    message: &str,
    text: &str,
) -> Result<(), GatewayError> {
    ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            record_in(
                tx,
                ingress.connection_name(),
                &serde_json::to_string(&attempt.payload.room)?,
                message,
                &attempt.delivery_id,
                text,
            )
        }))
    })
}

pub(crate) fn record_in(
    tx: &rusqlite::Transaction<'_>,
    connection: &str,
    room: &str,
    message: &str,
    delivery: &str,
    text: &str,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_whatsapp_outgoing VALUES(?1,?2,?3,?4,?5)",
        params![connection, room, message, delivery, text],
    )?;
    tx.execute(
        "UPDATE messaging_whatsapp_prompts SET message=?3 WHERE connection=?1 AND delivery=?2",
        params![connection, delivery, message],
    )?;
    Ok(())
}

pub(crate) fn confirmed_text(
    ingress: &Ingress,
    room: &Room,
    message: &str,
) -> Result<Option<String>, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row("SELECT text FROM messaging_whatsapp_outgoing WHERE connection=?1 AND room=?2 AND message=?3",
            params![ingress.connection_name(),serde_json::to_string(room)?,message], |row| row.get(0)).optional()?)
    })))
}

pub(crate) async fn observe_status(
    ingress: &Ingress,
    room: Room,
    provider: String,
    operation: Option<String>,
    recipient: Option<String>,
    time: i64,
    state: ProviderDeliveryState,
) -> Result<(), GatewayError> {
    let event = db(storage::key(
        "whatsapp-status",
        &(&room, &provider, &operation, &recipient, time, state),
    ))?;
    ingress
        .observe_delivery(EffectObservation {
            event,
            room,
            identity: operation.map_or_else(
                || EffectIdentity::ProviderId(provider.clone()),
                EffectIdentity::OperationKey,
            ),
            recipient,
            observed_at_ms: time,
            state,
            receipt: Some(provider),
        })
        .await?;
    Ok(())
}

pub(crate) fn mime(artifact: &AttachmentRef) -> (MediaKind, &'static str) {
    let (_, extension) = artifact.name().rsplit_once('.').unwrap_or(("", ""));
    match extension.to_ascii_lowercase().as_str() {
        "png" => (MediaKind::Image, "image/png"),
        "jpg" | "jpeg" => (MediaKind::Image, "image/jpeg"),
        "mp4" => (MediaKind::Video, "video/mp4"),
        "oga" | "ogg" | "opus" => (MediaKind::Audio, "audio/ogg"),
        "mp3" => (MediaKind::Audio, "audio/mpeg"),
        "wav" => (MediaKind::Audio, "audio/wav"),
        "pdf" => (MediaKind::Document, "application/pdf"),
        "txt" | "md" => (MediaKind::Document, "text/plain"),
        _ => (MediaKind::Document, "application/octet-stream"),
    }
}

pub(crate) fn retry(after: Duration) -> DeliveryResult {
    DeliveryResult::Retryable {
        after,
        scope: hermes_gateway_core::messaging::RetryScope::Part,
    }
}
