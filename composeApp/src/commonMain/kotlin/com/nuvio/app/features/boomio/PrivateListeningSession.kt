package com.nuvio.app.features.boomio

import kotlinx.coroutines.flow.StateFlow

/**
 * Roku-style private-listening state for the companion remote. The TV forks the
 * audio it is *already* decoding for its own speakers to this phone over direct
 * LAN UDP; the phone just receives it — no second stream is started (see the
 * nuvio-tv `docs/plan-private-listening-exo-tee.md` spec, R#n binding).
 */
enum class PrivateListeningStatus {
    /** Platform has no audio-fork receiver (e.g. iOS) — the UI hides the toggle. */
    Unsupported,

    /** Nothing is being forked to this phone. */
    Idle,

    /** `audio_fork_start` sent — waiting for the TV's ack. */
    Arming,

    /** The TV is streaming this phone's decoded audio; the receiver is live. */
    Active,

    /** `audio_fork_stop` sent / receiver tearing down. */
    Stopping,
}

/**
 * Why an arm attempt failed. Stable codes — the UI maps these to localized text,
 * so the controller (which can be platform-side) never handles strings.
 */
enum class PrivateListeningFailure {
    /** No playback is running on the TV to fork. */
    NoActivePlayer,

    /** Phone IP isn't on the TV's subnet (guest Wi-Fi / another VLAN). */
    DifferentNetwork,

    /** Malformed phone address — should not happen; the session resolves it. */
    BadAddress,

    /** The TV couldn't arm the fork (non-Exo engine, sink not live, already forked). */
    ForkUnavailable,

    /** No LAN address to advertise, or the UDP socket couldn't bind. */
    NoNetwork,

    /** The TV didn't ack the arm in time. */
    Timeout,

    /** The companion link dropped mid-arm / mid-stream. */
    ConnectionLost,
}

/** Snapshot the remote UI renders from [PrivateListeningSession.state]. */
data class PrivateListeningUiState(
    val status: PrivateListeningStatus = PrivateListeningStatus.Idle,
    /** `phoneIp:port` the TV is forking to while [PrivateListeningStatus.Active]. */
    val endpoint: String? = null,
    /** Last failure — kept until the next arm so the UI can explain a toggle. */
    val failure: PrivateListeningFailure? = null,
)

/**
 * Drives Roku-style private listening on this remote. Toggling arms asks the
 * paired TV (via [CompanionBridge]) to tee the audio it is decoding to a UDP
 * socket this phone binds; the TV's ack arrives back over the companion link
 * and the Android actual starts playing the PCM16 stream.
 *
 * Android actual binds the socket, plays through a low-latency
 * `android.media.AudioTrack`, and auto-tears-down when the companion link drops
 * (the hub's heartbeat expiry already stops the fork on the TV). Other
 * platforms report [PrivateListeningStatus.Unsupported].
 */
expect object PrivateListeningSession {
    val state: StateFlow<PrivateListeningUiState>

    /** Arm when idle/failed; disarm when arming/active/stopping. */
    fun toggle()

    /** Force-stop anything forked (user toggle / notification Stop / link drop). No-op when idle. */
    fun stop()

    /**
     * Persisted phone-vs-TV audio-sync offset in ms (positive = phone audio plays
     * later, relative to the ~100 ms baseline the receiver pre-fills). Setup-
     * dependent — tune on the Companion remote until the phone audio matches the
     * TV picture; the value survives restarts.
     */
    val syncOffsetMs: StateFlow<Int>

    /** Adjust [syncOffsetMs] (persisted, live-applied to an active fork). */
    fun setSyncOffsetMs(offsetMs: Int)
}
