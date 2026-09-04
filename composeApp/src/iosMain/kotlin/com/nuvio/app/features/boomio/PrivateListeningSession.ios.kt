package com.nuvio.app.features.boomio

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * iOS actual: no UDP audio-fork receiver is wired up on this platform — the
 * remote stays [PrivateListeningStatus.Unsupported] and the UI hides the toggle.
 */
actual object PrivateListeningSession {
    private val _state = MutableStateFlow(
        PrivateListeningUiState(status = PrivateListeningStatus.Unsupported),
    )

    actual val state: StateFlow<PrivateListeningUiState> = _state.asStateFlow()

    actual fun toggle() = Unit
    actual fun stop() = Unit
}
