// DAR modification: stream serializer tokens with standard iterators.
use std::iter;

use crate::access::NodeEdge;
use crate::id::{NameId, NamespaceId, PrefixId};
use crate::xmlvalue::Element;
use crate::xmlvalue::Value;
use crate::xotdata::{Node, Xot};

/// Output of serialization
///
/// Given an [`OutputToken`] or
/// [`PrettyOutputToken`](`crate::output::PrettyOutputToken`), this enum
/// represents what the token represents in the XML tree.
///
/// You can use this information for customized serialization.
#[derive(Debug, PartialEq)]
pub enum Output<'a> {
    /// Start tag open, i.e `<foo` or `<ns:foo`
    StartTagOpen(Element),
    /// Start tag close, either `>` or `/>`
    StartTagClose,
    /// End tag, i.e. `</foo>` or `</ns:foo>`
    EndTag(Element),
    /// Namespace declaration, i.e. `xmlns:foo="http://example.com"`
    Prefix(PrefixId, NamespaceId),
    /// Attribute, i.e. `foo="bar"`
    Attribute(NameId, &'a str),
    /// Text, i.e. `foo`
    Text(&'a str),
    /// Comment, i.e. `<!-- foo -->`
    Comment(&'a str),
    /// Processing instruction, i.e. `<?foo bar?>`
    ProcessingInstruction(NameId, Option<&'a str>),
}

pub(crate) fn gen_outputs(xot: &Xot, node: Node) -> impl Iterator<Item = (Node, Output)> + '_ {
    xot.traverse(node).flat_map(move |edge| {
        let (current, start, end) = match edge {
            NodeEdge::Start(current) => (current, Some(gen_edge_start(xot, node, current)), None),
            NodeEdge::End(current) => {
                let end = match xot.value(current) {
                    Value::Element(element) => Some(Output::EndTag(*element)),
                    _ => None,
                };
                (current, None, end)
            }
        };
        start
            .into_iter()
            .flatten()
            .chain(end)
            .map(move |output| (current, output))
    })
}

fn gen_edge_start(xot: &Xot, top_node: Node, node: Node) -> impl Iterator<Item = Output> + '_ {
    let value = xot.value(node);
    let scalar = match value {
        Value::Text(text) => Some(Output::Text(text.get())),
        Value::Comment(comment) => Some(Output::Comment(comment.get())),
        Value::ProcessingInstruction(pi) => {
            Some(Output::ProcessingInstruction(pi.target(), pi.data()))
        }
        _ => None,
    };
    let element = match value {
        Value::Element(element) => Some(element),
        _ => None,
    };
    scalar
        .into_iter()
        .chain(element.into_iter().flat_map(move |element| {
            // Preserve inherited prefixes when serializing a subtree.
            let inherited = (node == top_node).then(|| {
                xot.namespaces_in_scope(node)
                    .filter(move |&(prefix, _)| xot.get_namespace(node, prefix).is_none())
                    .map(|(prefix, namespace)| Output::Prefix(prefix, namespace))
            });
            let declared = xot
                .namespaces(node)
                .into_entries()
                .map(|(prefix, namespace)| Output::Prefix(prefix, *namespace));
            let attributes = xot
                .attributes(node)
                .into_entries()
                .map(|(name, value)| Output::Attribute(name, value));
            iter::once(Output::StartTagOpen(*element))
                .chain(inherited.into_iter().flatten())
                .chain(declared)
                .chain(attributes)
                .chain(iter::once(Output::StartTagClose))
        }))
}

/// Output token
///
/// This represents an [`Output`] as a rendered output token.
pub struct OutputToken {
    /// Whether the token is prefixed by a space character.
    pub space: bool,
    /// The token.
    ///
    /// This is a fragment of XML like `<foo` or `a="A"` or `/>`, etc.
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iter_mkgen() {
        let mut xot = Xot::new();
        let root = xot.parse(r#"<doc a="A">Text</doc>"#).unwrap();
        let a_id = xot.add_name("a");
        let doc = xot.document_element(root).unwrap();
        let doc_el = xot.element(doc).unwrap();
        let mut iter = gen_outputs(&xot, doc);

        let v = iter.next().unwrap().1;
        assert_eq!(v, Output::StartTagOpen(*doc_el));
        let v = iter.next().unwrap().1;
        assert_eq!(v, Output::Prefix(xot.xml_prefix(), xot.xml_namespace()));
        let v = iter.next().unwrap().1;
        assert_eq!(v, Output::Attribute(a_id, "A"));
        let v = iter.next().unwrap().1;
        assert_eq!(v, Output::StartTagClose);
        let v = iter.next().unwrap().1;
        assert_eq!(v, Output::Text("Text"));
        let v = iter.next().unwrap().1;
        assert_eq!(v, Output::EndTag(*doc_el));
        assert!(iter.next().is_none());
    }
}
