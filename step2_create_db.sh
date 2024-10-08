# for f in `ls *.sql | grep -v table`
# do
#     name=`echo ${f} | sed -r 's/.sql//'`
#     rm ${name}.db
#     cat tables.sql | sqlite3 ${name}.db
#     cat ${f} | sqlite3 ${name}.db
# done

rm ../../data/modules/gex/gex.db
cat tables.sql | sqlite3 ../../data/modules/gex/gex.db
cat ../../data/modules/gex/gex.sql | sqlite3 ../../data/modules/gex/gex.db