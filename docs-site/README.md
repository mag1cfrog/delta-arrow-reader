# Delta Arrow Reader Docs Site

Build the site locally with:

```bash
python -m pip install -r docs-site/requirements.txt && python -m zensical build --strict -f docs-site/mkdocs.yml
```

Serve it locally with:

```bash
python -m zensical serve -f docs-site/mkdocs.yml
```

## Content structure

- `Start here` teaches a Rust user how to install the crate and complete a
  first direct or DataFusion read.
- `How it works` explains the read path after a user has completed a
  quickstart.
- `Performance` records public benchmark results and their limits.
- `Reference` links to the generated Rust API on docs.rs.
- `Contributors` keeps extraction evidence and repository history out of the
  newcomer path.

Keep Delta Funnel workflow, Python, reporting, and SQL Server documentation in
the Delta Funnel site. This site owns the standalone reader's behavior.
