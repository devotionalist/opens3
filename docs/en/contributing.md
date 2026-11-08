> Language: [English](./contributing.md) · [中文](../zh/contributing.md)

# Contribution & commit rules

This repository is public. External contributors submit changes **only via pull requests**. Direct pushes to `master`, and creating tags / releases, are reserved for the repository owner (`@devotionalist`).

## Who can do what

| Action | Contributors | Owner (`@devotionalist`) |
|--------|--------------|--------------------------|
| Push directly to `master` | No | Yes (admin bypass) |
| Open a pull request | Yes (from a fork) | Yes |
| Approve / merge a PR into `master` | No (must wait for owner) | Yes |
| Create / update / delete tags | No | Yes |
| Publish GitHub Releases | No | Yes |

Enforced by GitHub [rulesets](https://github.com/devotionalist/opens3/rules) plus [`.github/CODEOWNERS`](../../.github/CODEOWNERS).

## Rules on `master`

1. **Pull request required** — no direct commits to `master` for non-admins.
2. **At least one approving review** — must include a **CODEOWNERS** review (`@devotionalist`).
3. **Stale reviews dismissed** — new pushes after approval clear previous approvals.
4. **Last push must be approved** — the person who pushes last still needs a fresh approval before merge.
5. **No force-push / no deleting `master`**.

## Tags and releases

- All tags (`refs/tags/*`) may be created, moved, or deleted **only by repository admins**.
- GitHub Releases should be cut by the owner only (typically from a tag they created).
- Do not ask collaborators for write access just to ship a release; open a PR and let the owner tag/release.

## Contributor workflow

```bash
# 1. Fork on GitHub, then clone your fork
git clone https://github.com/<you>/opens3.git
cd opens3
git remote add upstream https://github.com/devotionalist/opens3.git

# 2. Branch from up-to-date master
git fetch upstream
git checkout -b my-fix upstream/master

# 3. Develop, test, commit
cargo fmt
cargo clippy --workspace --all-targets
cargo test --workspace
# optional: bash tests/e2e_*.sh

# 4. Push to your fork and open a PR against devotionalist/opens3:master
git push -u origin my-fix
gh pr create --repo devotionalist/opens3 --base master
```

Then wait for **owner approval** and merge. Do not expect self-merge rights.

### PR checklist

- [ ] One logical change per PR when practical
- [ ] `cargo fmt` / `clippy` / tests green locally
- [ ] Docs updated under `docs/en/` (and `docs/zh/` when applicable)
- [ ] No secrets, credentials, or large binaries
- [ ] Commit messages describe **why**, not only what

## Owner notes

- Admin bypass on the branch ruleset allows the owner to push/hotfix `master` when needed.
- Prefer still using PRs for reviewable history, even for owner changes.
- Cut releases only after `master` is in the intended state; create the tag yourself, then publish the Release.

## Related docs

- [Development & testing](development.md) — layout, local loop, E2E
- [Architecture](architecture.md) — system design
- [Module docs](modules/README.md) — per-crate notes
