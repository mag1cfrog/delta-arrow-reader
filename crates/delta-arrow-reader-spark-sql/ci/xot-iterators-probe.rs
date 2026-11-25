use std::io::{self, Read};

fn main() -> io::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    for xml in input.split_terminator('\0') {
        let mut tree = xot::Xot::new();
        match tree.parse(xml) {
            Ok(root) => {
                let level_order: Vec<_> = tree.level_order(root).collect();
                let subtrees: Vec<_> = tree
                    .descendants(root)
                    .map(|node| {
                        let namespaces: Vec<_> = tree
                            .namespaces_in_scope(node)
                            .map(|(prefix, namespace)| {
                                (tree.prefix_str(prefix), tree.namespace_str(namespace))
                            })
                            .collect();
                        (namespaces, tree.to_string(node))
                    })
                    .collect();
                println!("{level_order:?} {subtrees:?}");
            }
            Err(error) => println!("{error:?}"),
        }
    }
    Ok(())
}
