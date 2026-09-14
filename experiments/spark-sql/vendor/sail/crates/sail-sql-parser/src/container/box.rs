// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::any::TypeId;

use crate::tree::{SyntaxDescriptor, SyntaxNode, TreeSyntax};

impl<T> TreeSyntax for Box<T>
where
    T: TreeSyntax + 'static,
{
    fn syntax() -> SyntaxDescriptor {
        SyntaxDescriptor {
            name: format!("Box({})", T::syntax().name),
            node: SyntaxNode::NonTerminal(TypeId::of::<T>()),
            children: vec![(TypeId::of::<T>(), Box::new(T::syntax))],
        }
    }
}
