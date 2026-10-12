# Dashboard

Proposed layout for the next human view. The page that ships today is the self-contained HTML in `crates/pulseblade-mcp/src/dashboard.html`: dark by default, with a light toggle. This document is the design after that.

## Open at a glance

Four tabs: **Overview**, **Fleet**, **Changes**, **Actions**. Dark is the default. Detail opens in a side panel; the list stays put.

**Overview** is an attention strip (failed, degraded, findings, awaiting approval, stale), the top problems, and one line per scope. When nothing is wrong, the page is the strip at zero and those scope lines.

**Fleet** groups machines under Kubernetes, Proxmox, and Hosts. Healthy groups stay collapsed. Each machine is one tile.

Screenshots are a sample fleet (`poc-apps`, `pve-01`, `ws-01`).

![Overview, dark: attention strip, top problems, one line each for Kubernetes, Proxmox, and Hosts](assets/dashboard/mock4-overview-dark.png)

*Overview, dark.*

![Overview, light: the same layout on the light theme](assets/dashboard/mock4-overview-light.png)

*Overview, light.*

![All quiet: zero counts and healthy scope lines](assets/dashboard/mock4-calm-dark.png)

*All quiet.*

![Fleet, dark: Kubernetes, Proxmox, and Hosts, one tile per machine](assets/dashboard/mock4-fleet-dark.png)

*Fleet. Healthy groups stay collapsed.*

![Detail, dark: fleet list with the side panel](assets/dashboard/mock4-panel-dark.png)

*Detail.*

![Overview on a 390px-wide screen](assets/dashboard/mock4-overview-390.png)

*Overview at 390px wide.*

## Avatar

Recommend **Orbit**: a node tower, a teal light, and an amber pulse arc whose bright head marks now.

![Orbit mark](assets/dashboard/pulseblade-mark.svg)

Orbit is the one that still reads at 16px, has no letters, and is not a weapon. **Lane** is the hero reduced to two pairs of towers and a curve; at 16px the towers turn to mush and the curve can read as a stray glyph. **Beat** is a heartbeat whose spike is a tower; it reads as generic health imagery and loses the tower at small sizes.

![Orbit, Lane, and Beat at 16 to 512 px on dark and light](assets/dashboard/brand-concepts.png)

*Orbit, Lane, and Beat.*

`pulseblade-mark.svg` is the square mark (`pulseblade-mark-light.svg` on a light header). `pulseblade-avatar.svg` is the full-bleed crop for a circle. `favicon-32.png` is the 32px favicon.

No text in the mark. No blades or weapons. No game imagery. No Greek motifs.

## System icons

Kubernetes, Proxmox, Linux and distro, and Docker icons are from [Simple Icons](https://simpleicons.org) (CC0) and name those integrated systems; generic line icons are original, and the trademarks stay with their owners.

## Data the real build still needs

- Per-node last-push age on the hub status API.
- A pod owner label.
- A pod node label.
- A host label on Proxmox nodes.
- A kind or label that distinguishes a Proxmox node from a Kubernetes node.
