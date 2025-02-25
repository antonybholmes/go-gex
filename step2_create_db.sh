# for f in `ls *.sql | grep -v table`
# do
#     name=`echo ${f} | sed -r 's/.sql//'`
#     rm ${name}.db
#     cat tables.sql | sqlite3 ${name}.db
#     cat ${f} | sqlite3 ${name}.db
# done

rm ../../data/modules/gex/gex.db
cat tables.sql | sqlite3 ../../data/modules/gex/gex.db

echo genes
cat ../../data/modules/gex/genes.sql | sqlite3 ../../data/modules/gex/gex.db
echo datasets
cat ../../data/modules/gex/datasets.sql | sqlite3 ../../data/modules/gex/gex.db
echo samples
cat ../../data/modules/gex/samples.sql | sqlite3 ../../data/modules/gex/gex.db
echo gex
cat ../../data/modules/gex/gex.sql | sqlite3 ../../data/modules/gex/gex.db