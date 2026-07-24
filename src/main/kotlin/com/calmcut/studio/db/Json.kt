package com.calmcut.studio.db

import kotlinx.serialization.json.Json

/** Single JSON instance for payload (de)serialisation across the persistence layer. */
val storyboardJson: Json = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}
