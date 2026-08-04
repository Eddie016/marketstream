# ADR 0003: Independent implementation

- Status: Accepted
- Date: 2026-08-04

## Context

The project is informed by distributed-systems coursework, but a public
portfolio repository must represent independent product and engineering work.
Publishing copied assignment scaffolding would weaken both authorship and the
project narrative.

## Decision

MarketStream is implemented in a clean repository with its own domain model,
tests, documentation, commit history, and product requirements. Course projects
may inform concepts, but files are not copied wholesale. Any third-party code or
data must retain its required attribution and license.

## Consequences

Some components will be reimplemented even when a coursework prototype exists.
The resume may accurately describe MarketStream as an independent extension of
distributed-systems coursework.
