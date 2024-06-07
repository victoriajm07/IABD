
REGISTER piggybank.jar
# Leemos los archivos
AIRPORTS = LOAD '$airports_file' USING
       org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
       AS (airportid:chararray, city:chararray, state:chararray, airportname:chararray);

FLIGHTS = LOAD '$flights_file' USING
       org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
       AS (dayofmonth:int, dayofweek:int, carrier:chararray, 
               depairportid:chararray, arrairportid:chararray, depdelay:int, arrdelay:int);

filtered = FILTER FLIGHTS BY arrdelay > 15;

grouped = GROUP filtered BY carrier;

counted = FOREACH grouped GENERATE group AS carrier, COUNT(filtered) AS total_retrasos;

ordered = ORDER counted BY total_retrasos DESC;

top5 = LIMIT ordered 5;

DUMP top5;
