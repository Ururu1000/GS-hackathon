// `MIGRATOR` embeds `../../migrations` via `sqlx::migrate!`; rebuild this crate whenever a
// migration is added or changed so new files are picked up (board #122).
fn main() {
    println!("cargo:rerun-if-changed=../../migrations");
}
