import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_USER     = "zakaria.lagdrori.29@edu.uiz.ac.ma"
SMTP_PASSWORD = "xeuh uwre tkdw wyox"  # replace with your app password

RECIPIENTS = [
    "morad.boussagman@gmail.com",
    "ayoubhmad55@gmail.com",
    "zakaria.lagdrori.29@edu.uiz.ac.ma",
    "youne.elma@gmail.com"
]

def email_alert(context):
    dag_id   = context.get("dag").dag_id
    task_id  = context.get("task_instance").task_id
    exec_date = context.get("execution_date")
    log_url  = context.get("task_instance").log_url
    exception = context.get("exception")

    subject = f"❌ Airflow Task Failed: {dag_id}.{task_id}"

    body = f"""
    <h3>Airflow Task Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {exec_date}</p>
    <p><b>Error:</b> {exception}</p>
    <p><b>Log URL:</b> <a href="{log_url}">{log_url}</a></p>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(RECIPIENTS)

    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())
        print(f"✓ Alert email sent to {RECIPIENTS}")
    except Exception as e:
        print(f"✗ Failed to send alert email: {e}")