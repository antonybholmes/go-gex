dir=data/modules/gex

python scripts/discover_gex.py --dir=${dir} 

 
rm ${dir}/gex.db
cat scripts/gex.sql | sqlite3 ${dir}/gex.db
cat ${dir}/gex.sql | sqlite3 ${dir}/gex.db
