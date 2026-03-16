/**
 * OBSOLETE - NE PAS UTILISER
 *
 * Ce fichier Apps Script a été remplacé par un Cloud Scheduler GCP
 * qui appelle directement le service Cloud Run en Python.
 * Le pipeline est 100% Python — aucun Apps Script n'est actif.
 *
 * Conservé uniquement à titre de référence historique.
 * ─────────────────────────────────────────────────────────────────────────
 * [ARCHIVE] Booking + Airbnb → PennyLane  |  Google Apps Script
 * Booking.com : tous les lundis à 8h (Europe/Paris)
 * Airbnb      : tous les jours  à 8h (Europe/Paris)
 * ─────────────────────────────────────────────────────────────────────────
 */

// ── Configuration ──────────────────────────────────────────────────────────

const CLOUD_RUN_URL     = 'https://booking-pipeline-1027120308924.europe-west1.run.app';
const BOOKING_FOLDER_ID = '1sTXjEQJZo6_DwTF_SMDumoNQTNINXMGe';  // compta/Paiements Booking
const AIRBNB_FOLDER_ID  = 'YOUR_AIRBNB_FOLDER_ID';   // à remplir quand Airbnb sera prêt
const TIMEZONE          = 'Europe/Paris';

// ── Installation des triggers ──────────────────────────────────────────────

/**
 * À exécuter UNE SEULE FOIS depuis l'éditeur Apps Script.
 * Supprime les anciens triggers puis en crée deux :
 *   - Booking  : chaque lundi  à 8h
 *   - Airbnb   : chaque jour   à 8h
 */
function installTriggers() {
  // Nettoyage des triggers existants
  ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));

  // Booking — chaque lundi à 8h
  ScriptApp.newTrigger('runBooking')
    .timeBased()
    .onWeekDay(ScriptApp.WeekDay.MONDAY)
    .atHour(8)
    .nearMinute(0)
    .inTimezone(TIMEZONE)
    .create();

  // Airbnb — chaque jour à 8h
  ScriptApp.newTrigger('runAirbnb')
    .timeBased()
    .everyDays(1)
    .atHour(8)
    .nearMinute(0)
    .inTimezone(TIMEZONE)
    .create();

  Logger.log('✓ Trigger Booking  : chaque lundi à 8h');
  Logger.log('✓ Trigger Airbnb   : chaque jour  à 8h');
}

// ── Handlers appelés par les triggers ─────────────────────────────────────

function runBooking() {
  _triggerPipeline(BOOKING_FOLDER_ID, 'booking');
}

function runAirbnb() {
  _triggerPipeline(AIRBNB_FOLDER_ID, 'airbnb');
}

// ── Appel du pipeline Cloud Run ────────────────────────────────────────────

function _triggerPipeline(folderId, ota) {
  const today = Utilities.formatDate(new Date(), TIMEZONE, 'yyyy-MM-dd');

  const payload = JSON.stringify({
    folder_id: folderId,
    date:      today,
    ota:       ota
  });

  const token = ScriptApp.getOAuthToken();

  const options = {
    method:             'post',
    contentType:        'application/json',
    payload:            payload,
    headers:            { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  };

  Logger.log('[' + ota + '] Appel Cloud Run — ' + today);
  const response = UrlFetchApp.fetch(CLOUD_RUN_URL + '/process', options);
  const code     = response.getResponseCode();
  const body     = response.getContentText();
  Logger.log('[' + ota + '] Réponse [' + code + '] : ' + body);

  if (code !== 200) {
    _sendErrorEmail(
      '[' + ota + '] Pipeline échoué [HTTP ' + code + ']',
      'Dossier : ' + folderId + '\nDate : ' + today + '\n\nRéponse :\n' + body
    );
    return;
  }

  const result = JSON.parse(body);

  if (result.status === 'blocked') {
    _sendErrorEmail(
      '[' + ota + '] Pipeline bloqué — ' + result.blocking + ' anomalie(s) bloquante(s)',
      result.blocking_details.join('\n')
    );
  } else if (result.status === 'skipped') {
    Logger.log('[' + ota + '] Aucun CSV trouvé dans le dossier — rien à faire.');
  } else {
    Logger.log(
      '[' + ota + '] OK — ' + result.reservations + ' réservations, ' +
      result.warnings + ' warnings, balance_ok=' + result.balance_ok
    );
  }
}

// ── Notification email en cas d'erreur ────────────────────────────────────

function _sendErrorEmail(subject, body) {
  const recipient = Session.getActiveUser().getEmail();
  if (recipient) {
    GmailApp.sendEmail(recipient, '[Pipeline Compta] ' + subject, body);
  }
}

// ── Tests manuels (sans attendre le trigger) ──────────────────────────────

/** Lance le pipeline Booking immédiatement (pour tester). */
function triggerBookingNow() {
  _triggerPipeline(BOOKING_FOLDER_ID, 'booking');
}

/** Lance le pipeline Airbnb immédiatement (pour tester). */
function triggerAirbnbNow() {
  _triggerPipeline(AIRBNB_FOLDER_ID, 'airbnb');
}
