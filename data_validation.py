import snowflake.connector
import smtplib
import datetime
from email.mime.text import MIMEText

def main():

    sender_email = "karthikwebscraping@gmail.com"
    reciver_email = "Karthikwebscraping@gmail.com"

    sfconn = snowflake.connector.connect(
        user="Dataengineering",
        password="<password>",
        account='<acc password>',
        warehouse="sample_wh",
    )


    cursor = sfconn.cursor()
    cursor.execute('USE warehouse COMPUTE_WH;')
    cursor.execute('USE DATABASE snowpipe_aws;')
    cursor.execute('USE SCHEMA PUBLIC;')

    cursor.execute('SELECT count(*) FROM aws_snowflake  WHERE ETL_INSERT_DATE > DATEADD(HOUR, -1, CURRENT_TIMESTAMP()) ORDER BY ETL_INSERT_DATE DESC;')
    row_count = cursor.fetchone()[0]
    print(row_count)

    threshold = 0

    try:    
        if row_count < threshold:
            subject = "Error :- No rows added "    
            Body = "No rows added"
        else:
            subject = f"""Subject: Success  
            {row_count} rows are inserted ."""
            Body = f"{row_count} rows are inserted ."

    except Exception as e:
        subject = f"Error"
        Body = f"Error :- {str(e)}"
    finally:
        cursor.close()

    send_email_alert(sender_email,reciver_email,subject,Body)

def send_email_alert(sender_email,reciver_email,subject,body):
    msg = MIMEText(body)
    msg["Subject"] =subject
    msg["From"] =sender_email
    msg["To"] = [reciver_email]

    smtp = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    
    smtp.login("karthikwebscraping@gmail.com", "zjxd lsde ladg kvyg")

    smtp.sendmail("karthikwebscraping@gmail.com", msg["To"], msg['Subject'])
    
    smtp.quit()

