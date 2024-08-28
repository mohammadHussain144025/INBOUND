#!/bin/bash

# Define variables
PGPASSWORD="mysecretpassword"
DOCKER_CONTAINER="spriced_postgres"  # Name of the Docker container running PostgreSQL
DB_HOST="172.17.234.157"              # Hostname of the PostgreSQL database
DB_PORT="5432"                       # Port number of the PostgreSQL database
DB_USER="postgres"                   # Username for accessing the PostgreSQL database
DB_NAME="spriced_meritor"                       # Name of the PostgreSQL database
BACKUP_FILE="/mnt/d/DB_BACKUPS/spriced_meritor/spriced_meritor_UAT_2024-07-30_0618.pgsql"   # Path to the backup file
LOG_FILE="/mnt/e/restore_log.log"

# Log function
log_message() {
    echo "$1 at $(date)" >> $LOG_FILE
}

# Execute pg_restore command within the PostgreSQL Docker container in the background
docker exec -i $DOCKER_CONTAINER psql -U $DB_USER -d $DB_NAME < $BACKUP_FILE &
RESTORE_PID=$!

# Function to check if the 'org_chart' table exists and has rows
check_org_chart_exists() {
    docker exec -i $DOCKER_CONTAINER psql -U $DB_USER -d $DB_NAME -tAc "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'org_chart');"
}

# Wait for the 'org_chart' table to be created and populated
while true; do
    if [ "$(check_org_chart_exists)" = 't' ]; then
        ROW_COUNT=$(docker exec -i $DOCKER_CONTAINER psql -U $DB_USER -d $DB_NAME -tAc "SELECT COUNT(*) FROM org_chart;")
        if [ "$ROW_COUNT" -gt 0 ]; then
            log_message "org_chart table exists with $ROW_COUNT rows"
            break
        fi
    fi
    log_message "Waiting for org_chart table to be restored..."
    sleep 10
done

# Update the 'org_chart' table with the specified values
UPDATE_QUERY="UPDATE org_chart SET 
    pmmi = 'shruthi.r@simadvisory.com',
    pami = 'yogesh.tayade@simadvisorypartner.com',
    pmlmi = 'krithika.k@simadvisory.com', 
    pdmi = 'krithika.k@simadvisory.com',
    pdl1mi = 'yogesh.tayade@simadvisorypartner.com',
    pdl2mi = 'yogesh.tayade@simadvisorypartner.com',
    edmi = 'yogesh.tayade@simadvisorypartner.com',
    prcmmi = 'yogesh.tayade@simadvisorypartner.com';"

if docker exec -i $DOCKER_CONTAINER psql -U $DB_USER -d $DB_NAME -c "$UPDATE_QUERY"; then
    log_message "org_chart table updated successfully"
else
    log_message "org_chart table update failed"
    exit 1
fi

# Verify the update
VERIFY_QUERY="SELECT pmmi, pami, pmlmi, pdmi, pdl1mi, pdl2mi, edmi, prcmmi FROM org_chart LIMIT 10;"
docker exec -i $DOCKER_CONTAINER psql -U $DB_USER -d $DB_NAME -c "$VERIFY_QUERY" > /mnt/d/Download/backup/verify_log.log

# Check if verification contains the correct email addresses
if grep -q "shruthi.r@simadvisory.com" /mnt/d/Download/backup/verify_log.log &&
   grep -q "yogesh.tayade@simadvisorypartner.com" /mnt/d/Download/backup/verify_log.log &&
   grep -q "krithika.k@simadvisory.com" /mnt/d/Download/backup/verify_log.log; then
    log_message "Update verified successfully, values are correct"
else
    log_message "Update verification failed, values are not as expected"
    exit 1
fi