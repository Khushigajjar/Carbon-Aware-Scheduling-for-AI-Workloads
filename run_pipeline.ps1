# Water-Energy Nexus — Daily Pipeline Runner
$projectPath = "C:\Users\Khushi Gajjar\Big_Data_Project"
Set-Location $projectPath

# Log file
$logFile = "$projectPath\pipeline_log.txt"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content $logFile "[$timestamp] Starting pipeline..."

# Step 1 - Ingest
python scripts/ingest_electricitymaps.py
python scripts/ingest_hubeau.py

# Step 2 - Spark formatting
docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/format_electricitymaps.py
docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/format_hubeau.py

# Step 3 - Combine
docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/combine.py


# Ensure elasticsearch is installed in Spark container
docker exec big_data_project-spark-1 pip3 install elasticsearch==8.12.0 --quiet

# Step 4 - Index
docker exec big_data_project-spark-1 /opt/spark/bin/spark-submit /scripts/index_elasticsearch.py

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content $logFile "[$timestamp] Pipeline complete!"