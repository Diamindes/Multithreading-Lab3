rmdir out /s /q
mkdir out
set MPJ_HOME=../mpj
javac -cp ./src/main/java;./mpj/lib/mpj.jar -d ./out ./src/main/java/Lab3.java
copy "./machines" "./out/"
cd out
%MPJ_HOME%/bin/mpjrun.bat -np %1 -Xmx1024m -Xms1024m Lab3 %2 %3 %4 %5
cd ..

:: .\run.bat 1 2 "../examples/sort-25000.txt" 25000 100000
:: .\run.bat 1 1 "../examples/sort-25000.txt" "../output-25000.txt"
:: %MPJ_HOME%/bin/mpjrun.bat -np %1 -dev niodev -src -wdir /temp/out/tosend Lab3 %2 %3 %4 %5
:: %MPJ_HOME%/bin/mpjrun.bat -np %1 -Xms2096M -Xmx2096M Lab3 %2 %3 %4 %5