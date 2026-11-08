//! Scriere CSV minimală (fără dependențe): antetul se scrie doar pentru fișiere noi.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub fn append(path: &Path, header: &[&str], rows: &[Vec<String>]) -> anyhow::Result<()> {
    let new = !path.exists() || std::fs::metadata(path)?.len() == 0;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    if new {
        writeln!(f, "{}", header.join(","))?;
    }
    for r in rows {
        debug_assert_eq!(r.len(), header.len());
        writeln!(f, "{}", r.join(","))?;
    }
    Ok(())
}
