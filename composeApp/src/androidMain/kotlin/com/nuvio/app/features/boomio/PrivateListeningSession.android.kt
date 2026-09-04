package com.nuvio.app.features.boomio

import co.touchlab.kermit.Logger
import java.net.DatagramSocket
import java.net.InetAddress
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout

/**
 * Android actual of [PrivateListeningSession].
 *
 * Owning the receive half of Roku-style private listening:
 *  - arm()  binds a UDP socket, learns this phone's LAN IPv4, advertises
 *           `ip:port` to the paired TV via [CompanionBridge.startAudioFork], then
 *           plays the PCM16 stream the TV forks back into a [ForkUdpReceiver].
 *  - The TV's `started`/`error` ack is awaited through a waiter registered
 *    synchronously before the send (see [CompanionBridge.registerAudioForkAckWaiter]),
 *    so the ack can never be dropped.
 *  - If the companion link drops mid-arm or mid-stream, the hub's heartbeat
 *    expiry has already stopped the fork on the TV; we tear the receiver down
 *    and report [PrivateListeningFailure.ConnectionLost].
 */
actual object PrivateListeningSession {

    private const val ACK_TIMEOUT_MS = 5_000L

    private val log = Logger.withTag("PrivateListeningSession")
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val _state = MutableStateFlow(PrivateListeningUiState())
    actual val state: StateFlow<PrivateListeningUiState> = _state.asStateFlow()

    private var ackJob: Job? = null
    private var linkWatchJob: Job? = null
    @Volatile private var receiver: ForkUdpReceiver? = null

    actual fun toggle() {
        if (_state.value.status == PrivateListeningStatus.Active ||
            _state.value.status == PrivateListeningStatus.Arming ||
            _state.value.status == PrivateListeningStatus.Stopping
        ) {
            stop()
        } else {
            arm()
        }
    }

    actual fun stop() {
        if (_state.value.status == PrivateListeningStatus.Idle) return
        ackJob?.cancel(); ackJob = null
        linkWatchJob?.cancel(); linkWatchJob = null
        receiver?.close(); receiver = null
        _state.value = PrivateListeningUiState(status = PrivateListeningStatus.Stopping)
        // Harmless when nothing armed; clears a partial arm / unwinds a live fork.
        CompanionBridge.stopAudioFork()
        _state.value = PrivateListeningUiState()
    }

    private fun arm() {
        ackJob?.cancel()
        linkWatchJob?.cancel()
        receiver?.close()
        _state.value = PrivateListeningUiState(status = PrivateListeningStatus.Arming)

        val ip = resolveLanIpv4()
        val socket = if (ip == null) null else runCatching { DatagramSocket(0) }.getOrNull()
        val port = socket?.localPort ?: 0
        if (ip == null || socket == null || port <= 0) {
            socket?.close()
            _state.value = PrivateListeningUiState(failure = PrivateListeningFailure.NoNetwork)
            return
        }

        val recv = ForkUdpReceiver(socket)
        receiver = recv
        recv.start()

        // Register the ack completer BEFORE sending so the TV's reply can never
        // land on an unsubscribed flow, then send the arm.
        val waiter = CompanionBridge.registerAudioForkAckWaiter()
        log.i { "Private listening arm -> $ip:$port" }
        CompanionBridge.startAudioFork(ip, port)

        // Tear down if the companion link dies at any point (the hub already
        // stopped the fork on the TV via its heartbeat timeout by then).
        linkWatchJob = scope.launch {
            CompanionBridge.connected.first { !it }
            log.i { "Companion link dropped — ending private listening" }
            ackJob?.cancel()
            teardown()
            _state.value = PrivateListeningUiState(failure = PrivateListeningFailure.ConnectionLost)
        }

        ackJob = scope.launch {
            val ack = try {
                withTimeout(ACK_TIMEOUT_MS) { waiter.await() }
            } catch (error: TimeoutCancellationException) {
                null
            }
            when {
                ack == null -> {
                    log.w { "audio_fork_start ack timeout (no reply in ${ACK_TIMEOUT_MS}ms)" }
                    teardown()
                    CompanionBridge.stopAudioFork() // the TV may have armed regardless
                    _state.value = PrivateListeningUiState(failure = PrivateListeningFailure.Timeout)
                }
                ack.status == "started" -> {
                    log.i { "Private listening active ($ip:$port)" }
                    _state.value = PrivateListeningUiState(
                        status = PrivateListeningStatus.Active,
                        endpoint = "$ip:$port",
                    )
                }
                else -> {
                    val failure = mapFailure(ack.reason)
                    log.w { "audio_fork_start rejected: ${ack.reason}" }
                    teardown()
                    _state.value = PrivateListeningUiState(failure = failure)
                }
            }
        }
    }

    private fun teardown() {
        ackJob?.cancel(); ackJob = null
        linkWatchJob?.cancel(); linkWatchJob = null
        receiver?.close(); receiver = null
    }

    private fun mapFailure(reason: String?): PrivateListeningFailure = when (reason) {
        "no_active_player" -> PrivateListeningFailure.NoActivePlayer
        "different_network" -> PrivateListeningFailure.DifferentNetwork
        "bad_phone_address" -> PrivateListeningFailure.BadAddress
        else -> PrivateListeningFailure.ForkUnavailable
    }

    /**
     * Best-effort LAN IPv4, resolved with the same default-route trick the TV's
     * `bestEffortLanIp()` uses, so both ends agree on which interface/subnet to
     * compare. `connect` performs no network I/O — it just pins the route so the
     * kernel reports the local address that would carry that traffic.
     */
    private fun resolveLanIpv4(): String? = try {
        DatagramSocket().use { probe ->
            probe.connect(InetAddress.getByName("8.8.8.8"), 10_002)
            probe.localAddress?.hostAddress?.takeIf { ip ->
                ip != "0.0.0.0" && ip != "127.0.0.1" && ip.count { it == '.' } == 3
            }
        }
    } catch (_: Throwable) {
        null
    }
}
