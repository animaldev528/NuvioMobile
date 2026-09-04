package com.nuvio.app.features.boomio

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.util.Log
import com.nuvio.app.R
import com.nuvio.app.core.build.AppFeaturePolicy

private const val TAG = "PrivateListeningFg"
private const val PL_CHANNEL_ID = "nuvio_private_listening"
private const val PL_NOTIFICATION_ID = 0x504C
private const val ACTION_START = "com.nuvio.app.privlistening.START"
private const val ACTION_STOP = "com.nuvio.app.privlistening.STOP"

/**
 * Foreground-service host for private listening. The actual audio work (UDP
 * receive + AudioTrack) lives on the threads of [PrivateListeningSession]; this
 * service exists so that when the phone leaves the Companion screen — or the OS
 * trims background processes — the process is held at media-playback foreground
 * priority and the fork keeps playing, exactly like a media player.
 *
 * Started by the session when the fork becomes [PrivateListeningStatus.Active],
 * stopped when the fork tears down (user toggle, link drop, swipe-away via
 * [onTaskRemoved], or the notification's Stop action). The notification doubles
 * as the "still listening" control surface: tapping it returns to the app.
 *
 * Mirrors the flavor gating of `PlayerNowPlayingService`: only wired in the
 * `full` flavor (where [AppFeaturePolicy.mediaPlaybackForegroundServiceEnabled]
 * is true and the manifest declares the service + FGS permissions).
 */
class PrivateListeningService : Service() {

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        createChannel(this)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val notification = PrivateListeningServiceState.consumeStartNotification()
                if (notification == null) {
                    stopSelf(startId)
                    return START_NOT_STICKY
                }
                val started = runCatching {
                    startForeground(PL_NOTIFICATION_ID, notification)
                }.onFailure { error ->
                    Log.w(TAG, "Unable to promote private-listening service", error)
                }.isSuccess
                if (!started) {
                    stopSelf(startId)
                    return START_NOT_STICKY
                }
                return START_NOT_STICKY
            }
            ACTION_STOP -> {
                // Notification Stop action — disarm the fork. stop() also stops this service.
                PrivateListeningSession.stop()
                stopSelf(startId)
                return START_NOT_STICKY
            }
            else -> {
                stopSelf(startId)
                return START_NOT_STICKY
            }
        }
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        // Swiped away from recents — the user is done with the app. Stop the fork
        // (the process may be reaped right after, so don't rely on onDestroy alone).
        PrivateListeningSession.stop()
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            stopForeground(STOP_FOREGROUND_REMOVE)
        } else {
            @Suppress("DEPRECATION")
            stopForeground(true)
        }
        getSystemService(NotificationManager::class.java)?.cancel(PL_NOTIFICATION_ID)
        PrivateListeningServiceState.clear()
        super.onDestroy()
    }
}

/** Bridge between [PrivateListeningSession] and the foreground service. */
internal object PrivateListeningForeground {

    /** Promote private listening to a foreground service. Safe to call while already up. */
    fun start(context: Context) {
        if (!AppFeaturePolicy.mediaPlaybackForegroundServiceEnabled) return
        val ctx = context.applicationContext
        createChannel(ctx)
        PrivateListeningServiceState.pendingNotification = buildNotification(ctx)
        val intent = Intent(ctx, PrivateListeningService::class.java).setAction(ACTION_START)
        runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                ctx.startForegroundService(intent)
            } else {
                ctx.startService(intent)
            }
        }.onFailure { error ->
            Log.w(TAG, "Unable to start private-listening service", error)
        }
    }

    /** Drop the foreground service + notification. Idempotent. */
    fun stop(context: Context) {
        val ctx = context.applicationContext
        PrivateListeningServiceState.clear()
        runCatching {
            ctx.stopService(Intent(ctx, PrivateListeningService::class.java))
        }.onFailure { error ->
            Log.w(TAG, "Unable to stop private-listening service", error)
        }
        runCatching {
            ctx.getSystemService(NotificationManager::class.java)?.cancel(PL_NOTIFICATION_ID)
        }
    }
}

private fun createChannel(context: Context) {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
    val manager = context.getSystemService(NotificationManager::class.java) ?: return
    val channel = NotificationChannel(
        PL_CHANNEL_ID,
        "Private listening",
        NotificationManager.IMPORTANCE_LOW,
    ).apply {
        description = "Audio streamed from the TV to this phone"
        setSound(null, null)
        enableVibration(false)
        setShowBadge(false)
        lockscreenVisibility = Notification.VISIBILITY_PUBLIC
    }
    manager.createNotificationChannel(channel)
}

private fun buildNotification(context: Context): Notification {
    val launchIntent = context.packageManager.getLaunchIntentForPackage(context.packageName)
        ?.addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP)
    val stopIntent = PendingIntent.getService(
        context,
        1,
        Intent(context, PrivateListeningService::class.java).setAction(ACTION_STOP),
        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
    )
    val builder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
        Notification.Builder(context, PL_CHANNEL_ID)
    } else {
        @Suppress("DEPRECATION")
        Notification.Builder(context)
    }
    return builder
        .setSmallIcon(R.drawable.ic_notification_small)
        .setContentTitle("Private listening")
        .setContentText("Audio from your TV is playing here")
        .setContentIntent(
            launchIntent?.let {
                PendingIntent.getActivity(
                    context,
                    0,
                    it,
                    PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
                )
            },
        )
        .setCategory(Notification.CATEGORY_TRANSPORT)
        .setVisibility(Notification.VISIBILITY_PUBLIC)
        .setOngoing(true)
        .setShowWhen(false)
        .setOnlyAlertOnce(true)
        .addAction(
            Notification.Action.Builder(
                android.R.drawable.ic_menu_close_clear_cancel,
                "Stop",
                stopIntent,
            ).build(),
        )
        .build()
}

/** Start-notification hand-off so [PrivateListeningService] can startForeground promptly. */
private object PrivateListeningServiceState {
    @Volatile var pendingNotification: Notification? = null

    fun consumeStartNotification(): Notification? = synchronized(this) {
        pendingNotification.also { pendingNotification = null }
    }

    fun clear() {
        pendingNotification = null
    }
}
