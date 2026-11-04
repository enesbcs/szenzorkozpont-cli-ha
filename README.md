# SzenzorKozpont — Home Assistant integration: installation via HACS (through a git repository)

> This document describes how to install the Home Assistant integration (`custom_components/szenzorkozpont/`) using HACS, through a git repository.

## 0. Prerequisites

- Home Assistant (minimum version: `2024.2.0`, see `hacs.json`).
- HACS installed and working in HA (HACS → Integrations).

## 1. Repository layout (what HACS expects)

Because of `content_in_root: false`, HACS looks for the `custom_components/` folder at the **root** of the repository:

```
<repo-root>/
├── hacs.json
├── README.md
└── custom_components/
    └── szenzorkozpont/
        ├── __init__.py
        ├── manifest.json
        └── ... (the integration files)
```

Important: the `custom_components/szenzorkozpont/` folder must be at the repository root (not nested inside another folder from HACS's point of view).

## 2. Installation via HACS (through a git repository)

1. Open the Home Assistant UI, then go to **HACS → Integrations**.
2. Top-right corner: **⋮ (three dots) → Custom repositories**.
3. Enter:
   - **Repository (URL):** the full git repository URL (e.g. `https://github.com/enesbcs/szenzorkozpont-cli-ha`).
   - **Category:** `Integration`.
   - Click **ADD**.
4. Find **SzenzorKozpont** in the list (based on the `name` field in `hacs.json`), then click **DOWNLOAD / INSTALL**.
5. Select the desired version (latest by default), then confirm the download.
6. **Restart Home Assistant** (Settings → System → Restart) so the new `custom_components` are loaded.
7. After restart: **Settings → Devices & Services → Add Integration** → find **SzenzorKozpont**.
8. In the Config Flow, provide:
   - **Server URL** (with port, e.g. `https://...`),
   - **API key**,
   - **Polling interval** (default: 10 min 06:00–21:00, 20 min 21:01–05:59).

## 3. Manual copy (without HACS) — paths

If you copy manually without HACS, the recommended locations are:

- **Home Assistant configuration directory** (where `configuration.yaml` lives):
  - Typical paths:
    - `/config/` (HA OS / Supervised / Docker default)
    - `/homeassistant/` (some installations)
    - `/usr/share/hassio/homeassistant/`
  - Target directory: `<config>/custom_components/szenzorkozpont/`
- **What to copy:** the entire contents of the `custom_components/szenzorkozpont/` folder (the integration `.py` files, `manifest.json`, `translations/`, etc.).
  - Do not copy the `hacs.json`, or `README.md` into `custom_components` — they are not needed at runtime (HACS does not put them there either).
- After copying, **restart Home Assistant**, then add the integration via Devices & Services (see steps 7–8 in section 2).

## 4. Updating

- Installed via HACS: HACS → Integrations → SzenzorKozpont → **UPDATE** button, then restart HA.
- Manually: overwrite the contents of `<config>/custom_components/szenzorkozpont/` with the newer version, then restart HA.

## 5. Uninstalling

- Via HACS: HACS → Integrations → SzenzorKozpont → **UNINSTALL**, then restart HA.
- Manually: delete the `<config>/custom_components/szenzorkozpont/` folder, then restart HA. Also remove the integration entries in Devices & Services.

## 6. Notes

- The `domain` in `manifest.json` (`szenzorkozpont`) must match the folder name; do not rename the folder.
- HACS identifies the repository based on `hacs.json`; because of `render_readme: true`, this README is displayed in the HACS UI.
- HACS always copies the repository's `custom_components/szenzorkozpont/` folder into the HA config's `custom_components/` directory.
