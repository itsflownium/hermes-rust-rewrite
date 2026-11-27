use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use reqwest::Url;

fn escape(output: &mut String, text: &str) {
    for character in text.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            _ => output.push(character),
        }
    }
}

fn link(url: &str) -> Option<Url> {
    let url = Url::parse(url).ok()?;
    (matches!(url.scheme(), "https" | "http")
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none())
    .then_some(url)
}

/// Render only Telegram's supported markup. A model's raw HTML and linked
/// image references stay inert. The provider receives no preview request.
pub(super) fn html(markdown: &str, rich: bool) -> Option<String> {
    let mut result = String::new();
    let mut links = Vec::new();
    let mut units = 0;
    for event in Parser::new_ext(markdown, Options::ENABLE_STRIKETHROUGH) {
        use Event::{End, Start};
        match event {
            Start(Tag::Paragraph) if rich => result.push_str("<p>"),
            End(TagEnd::Paragraph) if rich => result.push_str("</p>"),
            Start(Tag::Emphasis) => result.push_str("<i>"),
            End(TagEnd::Emphasis) => result.push_str("</i>"),
            Start(Tag::Strong | Tag::Heading { .. }) => result.push_str("<b>"),
            End(TagEnd::Strong) => result.push_str("</b>"),
            End(TagEnd::Heading(_)) => {
                result.push_str("</b>\n");
                units += 1;
            }
            Start(Tag::Strikethrough) => result.push_str("<s>"),
            End(TagEnd::Strikethrough) => result.push_str("</s>"),
            Start(Tag::CodeBlock(_)) => result.push_str("<pre><code>"),
            End(TagEnd::CodeBlock) => result.push_str("</code></pre>"),
            Start(Tag::Link { dest_url, .. }) => {
                let url = link(&dest_url);
                links.push(url.is_some());
                if let Some(url) = url {
                    result.push_str("<a href=\"");
                    escape(&mut result, url.as_str());
                    result.push_str("\">");
                }
            }
            End(TagEnd::Link) if links.pop() == Some(true) => result.push_str("</a>"),
            Event::Code(text) => {
                units += text.encode_utf16().count();
                result.push_str("<code>");
                escape(&mut result, &text);
                result.push_str("</code>");
            }
            Event::Text(text) | Event::Html(text) | Event::InlineHtml(text) => {
                units += text.encode_utf16().count();
                escape(&mut result, &text);
            }
            End(TagEnd::Paragraph) | Event::Rule => {
                result.push_str("\n\n");
                units += 2;
            }
            Start(Tag::Item) => {
                result.push_str("• ");
                units += 2;
            }
            End(TagEnd::Item) | Event::SoftBreak | Event::HardBreak => {
                result.push('\n');
                units += 1;
            }
            _ => {}
        }
    }
    let trimmed = result.trim_end();
    (units <= 4_096 && !trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_formatting_and_blocks_markup_or_links_from_unsafe_schemes() {
        let text = "🛰️ **bold** `x<y` [example](https://example.test) [bad](javascript:alert%281%29) <img src=x>";
        let formatted = html(text, false).unwrap();
        assert_eq!(
            formatted,
            "🛰️ <b>bold</b> <code>x&lt;y</code> <a href=\"https://example.test/\">example</a> bad &lt;img src=x&gt;"
        );
        assert_eq!(
            html("**bold**", true).as_deref(),
            Some("<p><b>bold</b></p>")
        );
    }
}
