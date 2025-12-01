dir=../data/modules/gex

python discover_gex.py --dir=${dir} 

 
rm ${dir}/gex.db
cat gex.sql | sqlite3 ${dir}/gex.db
cat ${dir}/gex.sql | sqlite3 ${dir}/gex.db
