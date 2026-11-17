# Issue tracker: Linear and GitHub PRs

Active requirements, implementation plans, acceptance criteria, dependencies, discussion and status live in Linear: team `SIP`, project `redis-stream-player`. Resolve an issue by its identifier (`SIP-NN`) or URL. GitHub PRs in `BuckanovNikita/redis-stream-player` own implementation review and delivery. Link issues and PRs in both directions.

Linear is private and this repository is public: PR descriptions, commit messages and committed files are readable by anyone.

**PRs as a request surface: no.** `/triage` reads this flag; a PR is a delivery of a Linear issue, not a request.

## Conventions

- **Ticket**: one Linear issue. Its description records the outcome, scope, an implementation plan where needed, acceptance criteria as a checklist, and the selected verification.
- **Feature**: a parent issue whose description is the spec, with its tickets as sub-issues.
- **Blocking**: Linear's native blocked-by relation.
- **State**: one lifecycle state (Backlog, Todo, In Progress, In Review, Done, Canceled, Duplicate) and one readiness label ([triage labels](triage-labels.md)). A ready label does not override an open blocker.
- **Discussion**: Linear comments on the issue.

## When a skill says "publish to the issue tracker"

Search Linear for an existing issue first, then create the issue in team `SIP`, project `redis-stream-player`.

## When a skill says "fetch the relevant ticket"

Read the issue by its identifier or URL, with its comments and relations.

## Lifecycle

Move an issue to In Progress when work starts and to In Review when the implementation and its selected verification are ready for review. Mark it Done only after its changes reach `main` through a merged PR and its acceptance criteria are satisfied. Record in the issue the checks actually run and their outcomes. Confirm a GitHub check, a branch protection or a Linear automation exists before relying on it.

## Delivery

- **Branch and PR**: one task branch and one PR against `main` for a cohesive change, with several tickets in it when they belong together. Name the issue identifier in the branch and in the PR, and link every included issue. A draft PR may hold incomplete work.
- **Who does what**: agents commit task-owned work with Conventional Commits, push task branches to `origin` and open PRs. Humans merge. `main` changes only through a human-merged PR: an agent session never pushes `main` and never delivers by merging into it locally. Explicit session limits take precedence.
- **Checks**: the [completion gate](../../AGENTS.md#completion-gate) passes on the branch before its PR is marked ready for review.
- **Review**: changes to behavior, interfaces, dependencies, security or operational policy get a fresh independent review after integrated verification. Wording edits may be self-reviewed.
