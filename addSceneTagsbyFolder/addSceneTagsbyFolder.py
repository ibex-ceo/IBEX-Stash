#!/usr/bin/env python3

import json
import os
import sys
import urllib.request


PLUGIN_KEY = "addSceneTagsbyFolder"


def plugin_result(output=None, error=None):
    result = {}
    if output is not None:
        result["output"] = output
    if error is not None:
        result["error"] = error
    print(json.dumps(result, indent=2))


def normalize_path(path):
    return path.replace("\\", "/").rstrip("/").lower()


def split_tags(value):
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return []


def graphql(url, query, variables=None, headers=None):
    payload = json.dumps({
        "query": query,
        "variables": variables or {}
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers=headers or {},
        method="POST"
    )
    req.add_header("Content-Type", "application/json")

    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode("utf-8"))

    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))

    return data["data"]


def get_graphql_connection(plugin_input):
    conn = plugin_input.get("server_connection", {})

    scheme = conn.get("Scheme", "http")
    port = conn.get("Port", 9999)

    url = f"{scheme}://localhost:{port}/graphql"

    headers = {}

    cookie = conn.get("SessionCookie")
    if cookie and cookie.get("Name") and cookie.get("Value"):
        headers["Cookie"] = f"{cookie['Name']}={cookie['Value']}"

    api_key = os.environ.get("STASH_API_KEY")
    if api_key:
        headers["ApiKey"] = api_key

    return url, headers


def get_plugin_config(url, headers):
    query = """
    query PluginConfig {
      configuration {
        plugins
      }
    }
    """

    data = graphql(url, query, headers=headers)
    plugins = data.get("configuration", {}).get("plugins", {})
    return plugins.get(PLUGIN_KEY, {})


def get_plugin_setting(url, headers, key, default=None):
    plugin_config = get_plugin_config(url, headers)

    # Stash stores plugin settings directly under the plugin key in this setup.
    # Some versions/configs may nest under "settings", so support both.
    if key in plugin_config:
        return plugin_config.get(key, default)

    return plugin_config.get("settings", {}).get(key, default)


def get_bool_setting(url, headers, key, default=False):
    value = get_plugin_setting(url, headers, key, default)

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y", "on")

    return bool(value)


def get_folder_tag_map_setting(url, headers):
    return get_plugin_setting(url, headers, "folder_tag_map", "")


def get_library_directories(url, headers):
    query = """
    query Configuration {
      configuration {
        general {
          stashes {
            path
          }
        }
      }
    }
    """

    data = graphql(url, query, headers=headers)
    stashes = data.get("configuration", {}).get("general", {}).get("stashes", [])

    paths = []
    for stash in stashes:
        path = stash.get("path")
        if path:
            paths.append(normalize_path(path))

    return sorted(set(paths))


def generate_template(url, headers):
    dirs = get_library_directories(url, headers)

    if not dirs:
        plugin_result(error="No library directories found.")
        return

    template = {directory: [] for directory in dirs}

    plugin_result({
        "message": "Copy this JSON into the folder_tag_map plugin setting.",
        "template": template
    })


def all_scenes(url, headers):
    query = """
    query FindScenes($page: Int!) {
      findScenes(
        scene_filter: {}
        filter: {
          page: $page
          per_page: 100
          sort: "path"
          direction: ASC
        }
      ) {
        count
        scenes {
          id
          title
          files {
            path
          }
          tags {
            id
            name
          }
        }
      }
    }
    """

    page = 1
    scenes = []

    while True:
        data = graphql(url, query, {"page": page}, headers)
        result = data["findScenes"]
        batch = result["scenes"]

        if not batch:
            break

        scenes.extend(batch)

        if len(scenes) >= result["count"]:
            break

        page += 1

    return scenes


def find_tag_by_name(url, headers, name):
    query = """
    query FindTags($name: String!) {
      findTags(
        tag_filter: { name: { value: $name, modifier: EQUALS } }
        filter: { per_page: 1 }
      ) {
        tags {
          id
          name
        }
      }
    }
    """

    data = graphql(url, query, {"name": name}, headers)
    tags = data["findTags"]["tags"]
    return tags[0]["id"] if tags else None


def create_tag(url, headers, name):
    mutation = """
    mutation TagCreate($input: TagCreateInput!) {
      tagCreate(input: $input) {
        id
        name
      }
    }
    """

    data = graphql(url, mutation, {"input": {"name": name}}, headers)
    return data["tagCreate"]["id"]


def get_or_create_tag(url, headers, name, cache, dry_run=False):
    key = name.lower()

    if key in cache:
        return cache[key]

    tag_id = find_tag_by_name(url, headers, name)

    if not tag_id:
        if dry_run:
            tag_id = f"DRY_RUN_NEW_TAG::{name}"
        else:
            tag_id = create_tag(url, headers, name)

    cache[key] = tag_id
    return tag_id


def sort_tag_ids(tag_ids):
    return sorted(
        list(tag_ids),
        key=lambda x: int(x) if str(x).isdigit() else str(x)
    )


def update_scene_tags(url, headers, scene_id, tag_ids):
    mutation = """
    mutation SceneUpdate($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) {
        id
      }
    }
    """

    graphql(
        url,
        mutation,
        {
            "input": {
                "id": scene_id,
                "tag_ids": sort_tag_ids(tag_ids)
            }
        },
        headers
    )


def parse_folder_map(raw_map):
    if not raw_map:
        raise ValueError("folder_tag_map is empty. Add JSON mapping in Settings > Plugins.")

    try:
        folder_map = json.loads(raw_map)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid folder_tag_map JSON: {e}")

    if not isinstance(folder_map, dict):
        raise ValueError("folder_tag_map must be a JSON object.")

    normalized_map = {
        normalize_path(folder): split_tags(tags)
        for folder, tags in folder_map.items()
    }

    normalized_map = {
        folder: tags
        for folder, tags in normalized_map.items()
        if folder and tags
    }

    if not normalized_map:
        raise ValueError("folder_tag_map does not contain any folders with tags assigned.")

    return folder_map, normalized_map


def apply_folder_tags(url, headers):
    raw_map = get_folder_tag_map_setting(url, headers)
    dry_run = get_bool_setting(url, headers, "dry_run", False)

    try:
        folder_map, normalized_map = parse_folder_map(raw_map)
    except ValueError as e:
        plugin_result(error=str(e))
        return

    scenes = all_scenes(url, headers)

    tag_cache = {}
    matched = 0
    updated = 0
    already_tagged = 0

    details = []

    for scene in scenes:
        file_paths = [
            normalize_path(file["path"])
            for file in scene.get("files", [])
            if file.get("path")
        ]

        matched_folders = []
        requested_tag_names = []

        for folder, tag_names in normalized_map.items():
            folder_prefix = folder + "/"

            if any(path == folder or path.startswith(folder_prefix) for path in file_paths):
                matched_folders.append(folder)
                requested_tag_names.extend(tag_names)

        if not requested_tag_names:
            continue

        matched += 1

        existing_tag_ids = {tag["id"] for tag in scene.get("tags", [])}
        existing_tag_names_lower = {
            tag.get("name", "").strip().lower()
            for tag in scene.get("tags", [])
        }

        new_tag_ids = set(existing_tag_ids)
        added_tag_names = []
        already_present_tag_names = []

        # De-duplicate requested tags while preserving order.
        seen_requested = set()
        requested_unique = []
        for tag_name in requested_tag_names:
            key = tag_name.lower()
            if key not in seen_requested:
                requested_unique.append(tag_name)
                seen_requested.add(key)

        for tag_name in requested_unique:
            if tag_name.lower() in existing_tag_names_lower:
                already_present_tag_names.append(tag_name)
                continue

            tag_id = get_or_create_tag(url, headers, tag_name, tag_cache, dry_run=dry_run)
            new_tag_ids.add(tag_id)
            added_tag_names.append(tag_name)

        if added_tag_names:
            updated += 1

            if not dry_run:
                update_scene_tags(url, headers, scene["id"], new_tag_ids)

            details.append({
                "scene_id": scene["id"],
                "title": scene.get("title", ""),
                "matched_folders": matched_folders,
                "file_paths": file_paths,
                "tags_added": added_tag_names,
                "already_present": already_present_tag_names,
                "action": "would_update" if dry_run else "updated"
            })
        else:
            already_tagged += 1
            details.append({
                "scene_id": scene["id"],
                "title": scene.get("title", ""),
                "matched_folders": matched_folders,
                "file_paths": file_paths,
                "tags_added": [],
                "already_present": already_present_tag_names,
                "action": "already_tagged"
            })

    if matched == 0:
        plugin_result({
            "dry_run": dry_run,
            "message": "No scenes matched any configured folders.",
            "configured_folders": list(folder_map.keys())
        })
        return

    plugin_result({
        "dry_run": dry_run,
        "summary": {
            "matched_scenes": matched,
            "updated_scenes": updated,
            "already_tagged_scenes": already_tagged,
            "total_scenes_scanned": len(scenes)
        },
        "configured_folders": list(folder_map.keys()),
        "details": details
    })


def get_task_name(plugin_input):
    return (
        plugin_input.get("args", {})
        .get("task", {})
        .get("name", "")
    )


def main():
    plugin_input = json.loads(sys.stdin.read() or "{}")

    task_name = get_task_name(plugin_input)
    url, headers = get_graphql_connection(plugin_input)

    if task_name == "Generate folder tag map template":
        generate_template(url, headers)
        return

    apply_folder_tags(url, headers)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        plugin_result(error=str(e))