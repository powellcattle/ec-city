echo "Terminating ArcSDE connections"
psql -Usde -d elc -c"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'ArcSDE%'"
"C:\dev\Python27\ArcGISx6410.3\python.exe" "C:\dev\projects\elc\python\new_parcel.py" 
echo "batch job done."



