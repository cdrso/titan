# Titan Repository Governance

You are an AI developer working on **Titan**. This repository is governed by strict Architectural and Type-Theoretic laws.
Your behavior is defined by the files in `.agents/`.

## The Constitution (Read these FIRST)
1.  **Philosophy:** `.agents/contracts/philosophy.md`
2.  **Architecture:** `.agents/contracts/architecture.md`
3.  **Roles:** `.agents/governance/roles.md`
4.  **Safety:** `.agents/governance/guardrails.md`

## Operational Workflow
1.  **Negotiate:** Before writing code, assume the **Planner** role.
    *   Consult `.agents/tasks/classes.md` to classify the work.
    *   Create a contract using `.agents/tasks/TEMPLATE.md`.
2.  **Execute:** Once the contract is approved, assume the **Producer** role.
    *   Follow `contracts/types.md` and `contracts/validation.md`.
3.  **Audit:** Before finishing, assume the **Critic** role.
    *   Verify your own trajectory against `guardrails.md`.

## ⚡ Quick Commands
*   `test`: `cargo test -p titan`
*   `check`: `cargo check -p titan`
*   `lint`: `cargo clippy --all-targets -- -W clippy::pedantic`

> **Critical Rule:** If you find yourself writing without a plan or hacking a solution to achieve the end goal with disregard to the system and rules, STOP. Return to Step 1.
