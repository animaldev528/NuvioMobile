package com.nuvio.app.features.boomio

import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioTrack
import android.os.Build
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

private const val PREAMBLE_BYTES = 7        // 4B sampleRate + 2B seq (BE) + 1B channel count
private const val MAX_DATAGRAM_BYTES = 4096 // TV sender caps datagrams at 1200 B; headroom to be safe
private const val MIN_SAMPLE_RATE = 8_000
private const val MAX_SAMPLE_RATE = 192_000
// Channel counts the wire may advertise (matches the TV's sender). 6/8 = raw multichannel
// passthrough — rendered via a 5.1/7.1 AudioTrack so Android's own mixer does the stereo fold.
private val SUPPORTED_CHANNEL_COUNTS = setOf(1, 2, 6, 8)
private const val BASE_PREFILL_MS = 100     // baseline playout prefill (play only after ~100 ms)
private const val MIN_PREFILL_MS = 20       // sync slider floor — below this the phone underruns
private const val MAX_PREFILL_MS = 500      // sync slider ceiling — beyond this it's unusable lag
private const val JITTER_CUSHION_MS = 120   // extra AudioTrack buffer above prefill for network jitter

/**
 * Receives the TV's private-listening PCM stream on a bound [DatagramSocket]
 * and plays it through a low-latency [AudioTrack].
 *
 * Wire format (from the nuvio-tv `PrivateListeningAudioSender`; each datagram is
 * self-describing so a late-joining phone syncs on its first packet and a
 * mid-stream rate change flows through):
 * ```
 *   bytes 0..3  sampleRate (big-endian int32)
 *   bytes 4..5  sequence   (big-endian uint16 — unused by playback, reserved for diagnostics)
 *   byte   6    channel count (1..8): 2 = stereo; 6/8 = raw multichannel passthrough
 *   bytes 7..   PCM16 signed little-endian interleaved, channel-count channels
 * ```
 *
 * TEMP diagnostic pairing: the TV sender redefines the wire to carry a channel count so that 5.1/7.1
 * decode can be passed through UNTOUCHED and this receiver renders it through a multichannel
 * AudioTrack, letting Android's own downmixer fold it to whatever the earbuds accept — an A/B
 * against the TV's hand-rolled fold. Each datagram is a whole number of frames, so writes are
 * always frame-aligned.
 *
 * The AudioTrack's own buffer IS the jitter buffer: it is sized to the playout
 * prefill plus a [JITTER_CUSHION_MS] cushion, and ~[BASE_PREFILL_MS] (+ the
 * user's audio-sync offset, see [setSyncOffsetMs]) is prefilled (written while
 * the track is still stopped) before [AudioTrack.play] is called, so network
 * jitter lands inside the cushion instead of popping the speaker. After play,
 * streaming `write()` blocks when the buffer is full, so the phone is paced by
 * the TV's decode clock and cannot drift (R4 in the spec).
 *
 * Audio sync: the phone is structurally a fixed offset behind the TV's decoded
 * moment, dominated by the prefill + OS output latency. The companion remote's
 * Audio sync slider shifts that offset in ~± steps around the baseline by
 * changing the prefill target; [setSyncOffsetMs] while playing tears the track
 * down and re-cushions to the new target (a brief gap, then audio at the new
 * delay). This corrects the setup-dependent TV-vs-phone lip-sync offset.
 *
 * All socket + AudioTrack work happens on a single daemon thread; [close]
 * unblocks the blocking receive by closing the socket, joins the thread, and
 * only then stops/releases the track — the track is never touched concurrently.
 */
internal class ForkUdpReceiver(private val socket: DatagramSocket) {

    private val closed = AtomicBoolean(false)
    @Volatile private var running = true

    @Volatile private var syncOffsetMs = 0

    private val buffer = ByteArray(MAX_DATAGRAM_BYTES)

    // AudioTrack state — touched only from the receiver thread (post-join in close()).
    private var track: AudioTrack? = null
    private var trackSampleRate = 0
    private var trackChannels = 2
    private var trackPlaying = false
    private var appliedPrefillMs = 0
    private var prefilledBytes = 0

    private val thread = Thread({ runLoop() }, "nuvio-pl-udp-receiver").apply { isDaemon = true }

    fun start() = thread.start()

    /** User audio-sync offset in ms (+ = later). Applied live; persists in the session. */
    fun setSyncOffsetMs(offsetMs: Int) {
        syncOffsetMs = offsetMs
    }

    fun close() {
        if (!closed.compareAndSet(false, true)) return
        running = false
        runCatching { socket.close() } // unblocks receive()
        runCatching { thread.join(1_500) }
        val current = track
        if (current != null) {
            runCatching { current.stop() }
            runCatching { current.release() }
            track = null
        }
    }

    private fun runLoop() {
        while (running) {
            val packet = DatagramPacket(buffer, buffer.size)
            try {
                socket.receive(packet)
            } catch (_: Exception) {
                if (!running) break
                continue
            }
            val len = packet.length
            if (len <= PREAMBLE_BYTES) continue
            val sampleRate = ByteBuffer.wrap(buffer, 0, 4).order(ByteOrder.BIG_ENDIAN).int
            if (sampleRate !in MIN_SAMPLE_RATE..MAX_SAMPLE_RATE) continue
            val channels = buffer[6].toInt() and 0xFF
            if (channels !in SUPPORTED_CHANNEL_COUNTS) continue
            val frameBytes = 2 * channels
            val payloadLen = len - PREAMBLE_BYTES
            // Whole frames only — AudioTrack.write must never be handed a partial frame.
            if (payloadLen < frameBytes || payloadLen % frameBytes != 0) continue
            writePcm(sampleRate, channels, payloadLen)
        }
    }

    private fun writePcm(sampleRate: Int, channels: Int, payloadLen: Int) {
        ensureTrack(sampleRate, channels)
        val current = track ?: return
        val wantPrefill = targetPrefillMs()
        val written = runCatching { current.write(buffer, PREAMBLE_BYTES, payloadLen) }.getOrDefault(0)
        if (written <= 0) return
        if (!trackPlaying) {
            prefilledBytes += written
            if (prefilledBytes >= msBytes(sampleRate, channels, wantPrefill)) {
                runCatching { current.play() }
                trackPlaying = true
            }
        }
    }

    /** The current prefill target: the 100 ms baseline plus the user's sync offset. */
    private fun targetPrefillMs(): Int =
        (BASE_PREFILL_MS + syncOffsetMs).coerceIn(MIN_PREFILL_MS, MAX_PREFILL_MS)

    private fun channelMaskFor(channels: Int): Int? = when (channels) {
        1 -> AudioFormat.CHANNEL_OUT_MONO
        2 -> AudioFormat.CHANNEL_OUT_STEREO
        6 -> AudioFormat.CHANNEL_OUT_5POINT1
        8 -> AudioFormat.CHANNEL_OUT_7POINT1_SURROUND
        else -> null
    }

    private fun frameBytes(channels: Int): Int = 2 * channels

    /**
     * Create (or rebuild) the [AudioTrack] for [sampleRate] and [channels]. Rebuilds when the
     * sample rate changes (mid-stream track change), the channel count changes (2.0 vs 5.1 track,
     * or passthrough toggled), OR the playout prefill target moves (audio-sync slider) — all reset
     * the cushion so playback re-syncs at the new latency.
     *
     * For multichannel (5.1/7.1) input the track is opened with that channel mask and the platform
     * mixer downmixes to the actual output (earbuds) — this is the reference fold used in the A/B.
     * Low-latency mode is only requested for stereo/mono; multichannel tracks take the default mode.
     */
    private fun ensureTrack(sampleRate: Int, channels: Int) {
        val wantPrefill = targetPrefillMs()
        if (track != null && trackSampleRate == sampleRate && trackChannels == channels &&
            appliedPrefillMs == wantPrefill
        ) return
        val previous = track
        if (previous != null) {
            runCatching { previous.stop() }
            runCatching { previous.release() }
        }
        track = null
        trackSampleRate = 0
        trackChannels = channels
        trackPlaying = false
        appliedPrefillMs = wantPrefill
        prefilledBytes = 0

        val mask = channelMaskFor(channels) ?: return
        val minBuffer = AudioTrack.getMinBufferSize(
            sampleRate,
            mask,
            AudioFormat.ENCODING_PCM_16BIT,
        )
        val minBufferSafe = if (minBuffer > 0) minBuffer else 4_000
        val bufferBytes = maxOf(
            minBufferSafe * 2,
            msBytes(sampleRate, channels, wantPrefill + JITTER_CUSHION_MS),
        )
        val attributes = AudioAttributes.Builder()
            .setUsage(AudioAttributes.USAGE_MEDIA)
            .setContentType(AudioAttributes.CONTENT_TYPE_MUSIC)
            .build()
        val format = AudioFormat.Builder()
            .setEncoding(AudioFormat.ENCODING_PCM_16BIT)
            .setSampleRate(sampleRate)
            .setChannelMask(mask)
            .build()
        val builder = AudioTrack.Builder()
            .setAudioAttributes(attributes)
            .setAudioFormat(format)
            .setTransferMode(AudioTrack.MODE_STREAM)
            .setBufferSizeInBytes(bufferBytes)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && channels <= 2) {
            builder.setPerformanceMode(AudioTrack.PERFORMANCE_MODE_LOW_LATENCY)
        }
        val built = runCatching { builder.build() }.getOrNull() ?: return
        if (built.state != AudioTrack.STATE_INITIALIZED) {
            runCatching { built.release() }
            return
        }
        track = built
        trackSampleRate = sampleRate
        trackChannels = channels
    }

    private fun msBytes(sampleRate: Int, channels: Int, ms: Int) =
        sampleRate * frameBytes(channels) * ms / 1_000
}
