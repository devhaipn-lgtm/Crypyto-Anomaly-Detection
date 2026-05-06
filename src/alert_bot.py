import time
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import clickhouse_connect
from datetime import datetime

# --- CONFIGURATION ---
# 1. Email Provider Settings (Example for Gmail)
# Note: For Gmail, you must enable 2-Step Verification and generate an 'App Password'.
# Go to Google Account -> Security -> 2-Step Verification -> App passwords.
SMTP_SERVER = 'mail.smtp2go.com'
SMTP_PORT = '2525'
SMTP_USER = 'milient-dev'  # For services like SMTP2GO where username differs from email
SENDER_EMAIL = 'thai.nguyen@milientsoftware.com'
SENDER_PASSWORD = '7TA6F9BhcjUmbeMf'

# 2. Receiver Settings
RECEIVER_EMAIL = 'Thai.NN252270M@sis.hust.edu.vn'

# ClickHouse Settings (use 'clickhouse' hostname inside Docker)
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '5'))
MAX_ALERTS = int(os.getenv('MAX_ALERTS', '1'))  # Maximum alerts to send

client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST)

def send_email_alert(subject, body):
    """Sends an email alert using SMTP."""
    if SENDER_PASSWORD == 'your_app_password':
        print(" Email not configured. Set SENDER_PASSWORD to enable alerts.")
        print(f"   Would send: {subject}")
        return False

    try:
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        # Connect to SMTP Server
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls() # Secure the connection
        login_user = SMTP_USER if SMTP_USER else SENDER_EMAIL
        server.login(login_user, SENDER_PASSWORD)
        text = msg.as_string()
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, text)
        server.quit()
        print(f"📧 Email sent to {RECEIVER_EMAIL}")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False

def test_alert():
    """Send a test alert to verify email configuration."""
    print("=" * 50)
    print("🧪 TESTING ALERT SYSTEM")
    print("=" * 50)
    print(f"SMTP Server: {SMTP_SERVER}:{SMTP_PORT}")
    print(f"Sender: {SENDER_EMAIL}")
    print(f"Receiver: {RECEIVER_EMAIL}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}")
    print(f"User: {SMTP_USER}")
    print("=" * 50)
    
    subject = "🧪 Test Alert - Crypto Anomaly Detection System"
    body = (
        "This is a TEST alert from your Crypto Anomaly Detection System.\n\n"
        "If you received this email, your alert configuration is working correctly!\n\n"
        f"Configuration:\n"
        f"- SMTP Server: {SMTP_SERVER}:{SMTP_PORT}\n"
        f"- Sender: {SENDER_EMAIL}\n"
        f"- Receiver: {RECEIVER_EMAIL}\n"
        f"- ClickHouse Host: {CLICKHOUSE_HOST}\n"
        f"- Check Interval: {CHECK_INTERVAL}s\n\n"
        "Your bot is ready to send anomaly alerts!"
    )
    
    print("\nSending test email...")
    success = send_email_alert(subject, body)
    
    if success:
        print("\n✅ Test successful! Check your inbox.")
    else:
        print("\n❌ Test failed. Check your configuration.")
    
    return success

def main():
    print(f"🤖 Crypto Email Alert Bot started. Monitoring ClickHouse every {CHECK_INTERVAL} seconds...")
    print(f"📧 Maximum alerts: {MAX_ALERTS}")
    print("Press Ctrl+C to stop.")
    
    # Track alerted anomalies by unique key (event_time + symbol)
    alerted_anomalies = set()
    
    # Track total alerts sent
    alert_count = 0

    while True:
        try:
            # Check if max alerts reached
            if alert_count >= MAX_ALERTS:
                print(f"⚠️  Maximum alerts ({MAX_ALERTS}) reached. Stopping alert bot.")
                break
            
            # Query non-alerted anomalies only
            query = """
                SELECT symbol, price, volume, event_time,
                       volume_score, sentiment_avg, combined_score
                FROM crypto.market_data
                WHERE is_anomaly = 1 AND is_alerted = 0
                ORDER BY event_time ASC
                LIMIT 10
            """
            
            result = client.query(query)
            rows = result.result_rows
                
            for row in rows:
                symbol, price, vol, event_time, vol_score, sent_avg, combined = row
                
                # Check alert limit
                if alert_count >= MAX_ALERTS:
                    print(f"⚠️  Max alerts ({MAX_ALERTS}) reached")
                    break
                
                # Determine trigger reason
                reasons = []
                if vol_score and vol_score >= 2.0:
                    reasons.append(f"Volume spike ({vol_score:.1f}σ above mean)")
                if sent_avg and sent_avg < -0.3:
                    reasons.append(f"Bearish sentiment (avg: {sent_avg:+.2f})")
                if not reasons:
                    reasons.append("Combined signal threshold exceeded")
                trigger_reason = ", ".join(reasons)
                
                # Format the message
                subject = f"🚨 Anomaly Detected: {symbol}"
                body = (
                    f"Anomaly Detected in Crypto Market\n\n"
                    f"Pair: {symbol}\n"
                    f"Price: ${price:,.2f}\n"
                    f"Volume: {vol:,.2f}\n"
                    f"Time: {event_time}\n\n"
                    f"--- Anomaly Score Breakdown ---\n"
                    f"Volume Score:    {vol_score:.2f} (Z-score, weight: 70%)\n"
                    f"Sentiment Avg:   {sent_avg:+.2f} ([-1 bearish, +1 bullish])\n"
                    f"Combined Score:  {combined:.2f} (threshold: 1.0)\n"
                    f"Trigger Reason:  {trigger_reason}\n\n"
                    f"Check your dashboard for more details.\n"
                    f"http://localhost:3000\n\n"
                    f"Alert {alert_count + 1} of {MAX_ALERTS}"
                )
                
                # Create unique key for tracking
                anomaly_key = f"{symbol}_{event_time}"
                
                # Skip if already processed in this session
                if anomaly_key in alerted_anomalies:
                    continue
                
                # Add to set BEFORE sending to prevent retries on failure
                alerted_anomalies.add(anomaly_key)
                
                print(f"📨 New anomaly! Sending alert {alert_count + 1}/{MAX_ALERTS} for {symbol} at {event_time}...")
                if send_email_alert(subject, body):
                    # Mark as alerted in ClickHouse using ALTER UPDATE
                    event_time_str = event_time.strftime('%Y-%m-%d %H:%M:%S')
                    update_query = f"""
                        ALTER TABLE crypto.market_data 
                        UPDATE is_alerted = 1 
                        WHERE event_time = '{event_time_str}' AND symbol = '{symbol}' AND is_anomaly = 1
                    """
                    try:
                        client.command(update_query)
                    except Exception as e:
                        print(f"Warning: Could not update DB: {e}")
                    
                    alert_count += 1
                    print(f"✅ Alert sent & marked in DB. Total: {alert_count}/{MAX_ALERTS}")
                else:
                    print(f"⚠️  Email failed, will not retry this anomaly")

            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            print("\nStopping bot...")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        test_alert()
    else:
        main()