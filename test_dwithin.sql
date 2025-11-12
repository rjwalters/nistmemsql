SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 2.0);
SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(10 10)'), 5.0);
