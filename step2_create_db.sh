for f in `find data/modules/gex/RNA-seq | grep sql`
do
    name=`echo ${f} | sed -r 's/.sql//'`
    rm ${name}.db
    cat rna.sql | sqlite3 ${name}.db
    cat ${f} | sqlite3 ${name}.db
    cat rna_indexes.sql | sqlite3 ${name}.db
done


exit(0)

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