spark-submit --name BLSSpeller \
--class be.ugent.intec.ddecap.BlsSpeller \
target/scala-2.11/bls-speller-assembly-0.1.jar \
--AB \
-i ../suffixtree-motif-speller/suffixtree/groupOrtho1.txt \
-o /tmp/bls-speller-spark_test \
-b ../suffixtree-motif-speller/suffixtree \
-p 2 \
--alphabet 1 \
--degen 1 \
--min_len 8 \
--max_len 9 \
--persist_level mem