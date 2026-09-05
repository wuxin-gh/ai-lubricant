# Skill Search SOP

## Purpose

When the Agent lacks a verified reusable procedure, search the platform's Skill marketplace/catalog instead of guessing or inventing a tool.

## Flow

1. Search the platform Skill catalog/marketplace for the task domain.
2. Inspect the Skill description, required capabilities, trust/source metadata and recent revision.
3. Ask the user before binding or enabling a Skill that adds capabilities, external access or side effects.
4. After binding, read its SOP with `file_read` and follow its stated limits.
5. Verify a successful use, then call `start_long_term_update` to distill the Agent-specific reusable procedure into L3.

## Rules

- Platform Skills are managed resources; they are not automatically injected into every Agent.
- Do not install arbitrary code or external Skill packages without authorization.
- Prefer a narrow, auditable Skill over broad access.
- Preserve the source and verification result when learning from a Skill.
