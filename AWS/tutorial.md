CONFIGURAZIONE AWS CLIENT IN LOCALE
1. https://awscli.amazonaws.com/AWSCLIV2.msi (download cli) -> installa cli
2. AWS learner Lab -> AWS details -> AWS Cli -> Copia contenuto
3. cmd -> aws configure -> inserire access_key, secret_access_key, session_token, region: us-east-1, output: json

SETUP DELL'AMBIENTE (caricamento Dataset e codice Job MR)
1. AWS learner Lab -> Start Lab -> AWS (Console) -> CloudShell
2. aws s3 mb s3://used-cars-big-data-analysis-$(date +%s) -> make_bucket: used-cars-big-data-analysis-1748698020 -> 1748698020 è l'id univoco, cambialo se crei un nuovo bucket
3. cmd (nella cartella del progetto) -> aws s3 cp data/used_cars_filtered.csv s3://used-cars-big-data-analysis-1748698020/data/
3.1 oppure tramite CloudShell:
	cat > used_cars_sample.csv << 'EOF'
	city,daysonmarket,description,make_name,model_name,price,year,description_cleaned
	New York,30,reliable car excellent condition,Toyota,Camry,15000,2015,reliable car excellent condition
	Chicago,45,powerful truck great for work,Ford,F150,25000,2018,powerful truck great for work
	Los Angeles,20,fuel efficient compact car,Honda,Civic,12000,2014,fuel efficient compact car
	Houston,60,luxury suv premium features,BMW,X3,35000,2019,luxury suv premium features
	Miami,25,sporty sedan fast acceleration,Nissan,Altima,18000,2016,sporty sedan fast acceleration
	EOF
	-> aws s3 cp used_cars_sample.csv s3://used-cars-big-data-analysis-1748698020/data/
4. mkdir -p scripts/job1/mapreduce -> creare mapper e reducer per entrambi i job:
	cat > scripts/job1/mapreduce/mapper.py << 'EOF' "codice" EOF -> chmod +x scripts/job1/mapreduce/mapper.py
	cat > scripts/job1/mapreduce/reducer.py << 'EOF' "codice" EOF -> chmod +x scripts/job1/mapreduce/reducer.py
	cat > scripts/job2/mapreduce/mapper.py << 'EOF' "codice" EOF -> chmod +x scripts/job2/mapreduce/mapper.py
	cat > scripts/job2/mapreduce/reducer.py << 'EOF' "codice" EOF -> chmod +x scripts/job2/mapreduce/reducer.py
5. test di funzionamento dei job: 
	cat used_cars_sample.csv | python3 scripts/job1/mapreduce/mapper.py | sort | python3 scripts/job1/mapreduce/reducer.py
	cat used_cars_sample.csv | python3 scripts/job2/mapreduce/mapper.py | sort | python3 scripts/job2/mapreduce/reducer.py

CREAZIONE CLUSTER EMR
1. aws emr create-cluster \
    --name "used-cars-mapreduce-cluster" \
    --release-label emr-6.4.0 \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --applications Name=Hadoop Name=Spark \
    --ec2-attributes KeyName=vockey,InstanceProfile=EMR_EC2_DefaultRole \
    --service-role EMR_DefaultRole \
    --log-uri s3://used-cars-big-data-analysis-1748698020/logs/ \
    --enable-debugging \
    --auto-terminate
	-> instance count determina il numero di macchine nel cluster (1 master + n-1 slave)
	-> si ottiene una cosa del tipo
		"ClusterId": "j-1BISU2UBTDQN4",
    	"ClusterArn": "arn:aws:elasticmapreduce:us-east-1:367845254606:cluster/j-1BISU2UBTDQN4"
2. carica script sul cluster -> aws s3 cp scripts/ s3://used-cars-big-data-analysis-1748698020/scripts/ --recursive
3. watch -n 30 'aws emr describe-cluster --cluster-id j-1BISU2UBTDQN4 --query "Cluster.Status.State"' -> premi ctrl+c quando il cluster è in stato 'WAITING'
	3.1. per terminare il cluster aws emr terminate-clusters --cluster-ids j-1BISU2UBTDQN4

ESECUZIONE JOB MR SUL CLUSTER
1.	aws emr add-steps --cluster-id j-2FMD9F0UC9J3S --steps '[
{
  "Name": "Job1-MakeModel-Analysis",
  "ActionOnFailure": "CONTINUE",
  "Jar": "command-runner.jar",
  "Args": [
    "hadoop-streaming",
    "-files", "s3://used-cars-big-data-analysis-1748698020/scripts/job1/mapreduce/mapper.py,s3://used-cars-big-data-analysis-1748698020/scripts/job1/mapreduce/reducer.py",
    "-mapper", "python3 mapper.py",
    "-reducer", "python3 reducer.py",
    "-input", "s3://used-cars-big-data-analysis-1748698020/data/used_cars_sample.csv",
    "-output", "s3://used-cars-big-data-analysis-1748698020/output/job1/"
  ]
}]'
2. risultato del tipo 
	{
		"StepIds": [
			"s-08704754X5MATCPWO4F"
		]
	}
3. watch -n 10 'aws emr describe-step --cluster-id j-2FMD9F0UC9J3S --step-id s-08704754X5MATCPWO4F --query "Step.Status.State"' -> aspetta che il job sia COMPLETED e fai ctrl+c
4. aws s3 ls s3://used-cars-big-data-analysis-1748698020/output/job1/ -> aws s3 cp s3://used-cars-big-data-analysis-1748698020/output/job1/part-00000 - | head -10


RUN DEL BENCHMARK COMPLETO (assicurati che il dataset completo sia stato caricato)
1. caricare script completo sul bucket S3 -> cat > full_benchmark_emr.py << 'EOF' "codice" EOF -> chmod +x full_benchmark_emr.py
	1.1. Salvalo su S3
		-> aws s3 cp full_benchmark_emr.py s3://used-cars-big-data-analysis-1748698020/scripts/
	1.2. Quando serve, scaricalo e caricalo sulla shell
		-> aws s3 cp s3://used-cars-big-data-analysis-1748698020/scripts/full_benchmark_emr.py ./
		-> chmod +x full_benchmark_emr.py
2. python3 full_benchmark_emr.py




# Esegui Spark/SparkSQL
aws emr add-steps --cluster-id j-2XGP9KTNXTHKC --steps '[{
  "Name": "Test-Job1-Spark-MakeModel-Analysis",
  "ActionOnFailure": "CONTINUE",
  "Jar": "command-runner.jar",
  "Args": [
    "spark-submit",
    "--deploy-mode", "cluster",
    "--conf", "spark.sql.adaptive.enabled=true",
    "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
    "s3://used-cars-big-data-analysis-1748698020/scripts/job1/spark/job1_spark.py",
    "s3://used-cars-big-data-analysis-1748698020/data/used_cars_fake.csv",
    "s3://used-cars-big-data-analysis-1748698020/output/test-spark-job1/"
  ]
}]'

# Monitora l'esecuzione dei job
watch -n 10 'aws emr describe-step --cluster-id j-X64BGHVG22L6 --step-id s-1030081PSYFYFT67XNK --query "Step.Status.State"'

# Visualizza i risultati di Job1 Spark SQL
aws s3 cp s3://used-cars-big-data-analysis-1748698020/output/test-sparksql-job1/part-00000-3d96b54c-24b8-44ae-aa28-baa167d932cc-c000.txt - | head -10