# SSH Open Marketplace — Curation Notebooks

A Python library and set of Jupyter notebooks for the SSH Open Marketplace editorial team. Part of a three-component curation setup:

| Component | Purpose | Location |
|-----------|---------|----------|
| **Curation Notebooks** *(this repo)* | Hands-on exploration and understanding of MP data; export tables for review | `marketplace-curation/` |
| **SSHOMPitor Dashboard** | Automated regular quality monitoring with exportable reports | [sshoc.github.io/sshompitor](https://sshoc.github.io/sshompitor/dashboard_output/metadata_dashboard_table.html) |
| **Curation Toolkit** | Complex write-back operations — delete/merge actors, anything requiring authentication | https://github.com/mkrzmr/curation-toolkit |

---

## Library — `sshmarketplacelib`

The library provides three classes. All write-back operations check `self.debug` before touching the live API. Set `DEBUG: False` in `config.yaml` to enable them (the default is `True`).

### `MPData` — data access and write-back

```python
from sshmarketplacelib import MPData as mpd
mpdata = mpd()
```

**Downloading data**

| Method | Description |
|--------|-------------|
| `getMPItems(category, local, pages)` | Download an item category; cache as `.pickle` in `data/`. Categories: `toolsandservices`, `publications`, `trainingmaterials`, `workflows`, `datasets`, `actors`. Set `local=True` to reuse the cached copy. |
| `getAllProperties()` | Normalise the `properties` list-of-dicts column across all cached category pickles. |
| `getMPKeywordProperties(keyword)` | Return items that carry a given keyword. |
| `getMPConcepts()` | Fetch all concept/vocabulary entries from the API. |

**Actors**

| Method | Description |
|--------|-------------|
| `getItemsforActor(actorid)` | `GET /api/actors/{id}?items=true` — items linked to an actor. |
| `postMergedActors(keep_id, merge_ids_csv)` | `POST /api/actors/{keep_id}/merge?with={ids}` — merge duplicate actors. Requires `DEBUG: False`. |

**Item merge**

| Method | Description |
|--------|-------------|
| `getMergedItem(category, pids)` | Preview a merged item without writing it. |
| `postMergedItem(item, pids)` | Write the merged item to the API. Requires `DEBUG: False`. |

**Curation flags (write-back)**

| Method | Description |
|--------|-------------|
| `setHTTPStatusFlags(dataset, flag, detail)` | Add URL-status curation flags to items. |
| `setURLStatusFlags(dataset, category, flag, detail)` | Add URL-specific flags. |
| `setPropertyFlags(dataset, flag, detail)` | Add property-level curation flags. |
| `updatePropertyValues(dataset, category, prop)` | Update a property value on live items. |
| `removePropertyFlag(dataset, flag, detail)` | Remove a curation flag. |
| `updateItems(dataset, updateList, filterList)` | Bulk-update item fields. |
| `restoreItems(items)` | Revert items to an earlier version. |

**Authentication**

| Method | Description |
|--------|-------------|
| `getBearer(entryPoint)` | Return a bearer token for the configured user. |

---

### `Util` — analysis helpers

```python
from sshmarketplacelib import helper as hel
utils = hel.Util()
```

Most methods read from the local snapshot file (`data/full_items_*.json`). Save the snapshot after loading items — see [Setup pattern](#setup-pattern) below.

**Data overview**

| Method | Description |
|--------|-------------|
| `getAllItemsBySources()` | Item counts grouped by data source. |
| `getItemsBySources(category)` | Same, filtered to one category. |
| `getCategoriesBySources()` | Category breakdown by source. |
| `getContributors()` | All actor–item links from the snapshot (one row per contribution). |

**Properties**

| Method | Description |
|--------|-------------|
| `getAllProperties(dataset)` | Explode and normalise the `properties` column of a DataFrame. |
| `getAllPropertiesBySources(dataset)` | Same, grouped by data source. |
| `getProperties(dataset)` | Property values for a filtered dataset. |
| `getPropertiesValuesFrequency(category, prop)` | Value-count distribution for one property. |

**Quality checks**

| Method | Description |
|--------|-------------|
| `getItemsWithNullValues(props, all)` | Items that are missing one or more specified fields. |
| `find_items_missing_profile(df)` | Validate items against their category's metadata profile; returns per-item `missing_fields` and a `score` (0–100). |

**Duplicates**

| Method | Description |
|--------|-------------|
| `getDuplicates(dataset, props)` | Rows in `dataset` that share the same value(s) in `props`. |
| `getDuplicatedActorsWithItems(dataset, props)` | Duplicate-name actor groups that have at least one associated item. Returns `(detail_df, summary_df)`. |

**Relations**

| Method | Description |
|--------|-------------|
| `getRelatedItems(categories, *n)` | Items and their related-item links. |
| `getAllRelatedItems()` | Same across all categories. |

**Utilities**

| Method | Description |
|--------|-------------|
| `make_clickable(val)` | Format a Marketplace URL as a clickable HTML link (for `DataFrame.style`). |
| `lists_to_list(nested)` | Flatten a list of lists. |

---

### `URLCheck` — HTTP status checking

```python
from sshmarketplacelib import eval as eva
check = eva.URLCheck()
```

| Method | Description |
|--------|-------------|
| `checkURLValues(categories, props)` | Check HTTP status of URL properties across one or more item categories. |
| `checkURLValuesInDataset(dataset, props)` | Same, on a pre-filtered DataFrame. |
| `getHTTP_Status(url)` | Check a single URL; maps connection errors to synthetic HTTP codes. |

---

## Setup pattern

After downloading items, save a combined JSON snapshot so `Util` methods can read it:

```python
import pandas as pd
from datetime import datetime
from sshmarketplacelib import MPData as mpd, helper as hel

mpdata = mpd()
utils  = hel.Util()

# Download (or load from cache)
df_tools     = mpdata.getMPItems("toolsandservices",  True)
df_pubs      = mpdata.getMPItems("publications",      True)
df_training  = mpdata.getMPItems("trainingmaterials", True)
df_workflows = mpdata.getMPItems("workflows",         True)
df_datasets  = mpdata.getMPItems("datasets",          True)

# Save combined snapshot — required by Util helper methods
_all = pd.concat([df_tools, df_pubs, df_training, df_workflows, df_datasets], ignore_index=True)
_all.to_json(f"data/full_items_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", orient="records")

# Now Util methods work
contributors = utils.getContributors()
```

---

## Notebooks

### `1. MarketplaceDataAnalysisAndOverview.ipynb`

A read-only overview of the full Marketplace dataset, useful for understanding data quality before prioritising curation work.

| Section | Content |
|---------|---------|
| 1 | Item provenance — system-imported vs. manually entered |
| 2 | Items by category and source |
| 3 | Duplicate items (label, URL, properties) |
| 4 | Metadata completeness — validation against category profiles |
| 5 | Items with missing values |
| 6 | Contributor / actor overview |
| 7 | Related-item network |
| 8 | Property value distributions |

### `2. ActorsCuration_workinprogress.ipynb`

Step-by-step actor curation. Each section is self-contained and can be run independently.

| Section | Content |
|---------|---------|
| 1 | Actors with a comma in the name field (likely multiple people entered as one) |
| 2 | Orphaned actors — snapshot cross-reference → live API verification → delete |
| 3 | Duplicate actors — find by name → inspect side-by-side → merge |
| 4 | Actor website URL status — identify broken links |
| 5 | External ID cleanup — detect identifiers stored as full URLs instead of bare IDs |

### `3. Duplicated_Item_process.ipynb`

Identify duplicate items and produce a merged item via `getMergedItem` / `postMergedItem`.

---

## Installation

A virtual environment is recommended to avoid dependency conflicts.

```bash
git clone https://github.com/SSHOC/marketplace-curation.git
cd marketplace-curation
pip install ./ -r ./requirements.txt
```

**Configuration:**

1. Copy `config.yaml.example` to `config.yaml` and fill in your API credentials and server URL.
2. Create a `data/` directory next to your notebooks.
3. Keep `DEBUG: True` (the default) while exploring. Set `DEBUG: False` only when you intend to write back to the live API.

```yaml
DEBUG: True   # set to False to enable write-back operations

API:
  SERVER: https://marketplace-api.sshopencloud.eu/
  USER: your-username
  PASSWORD: your-password

MARKETPLACE:
  SERVER: https://marketplace.sshopencloud.eu/
```

The `DATASET_ENTRYPOINTS`, `CATEGORIES`, `CATEGORY_FILTER_VALUES`, and `EMPTY_DESCRIPTION_VAL` sections in the example file should not need to be changed.

**Dependencies:** `pandas`, `numpy`, `requests`, `PyYAML`, `bokeh`
