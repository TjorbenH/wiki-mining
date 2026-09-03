# wiki-mining

Testing website: books.toscrape.com

## datei upload 
Schritt 1: Datensatz in Postgres anlegen (Status: pending oder einfach noch ohne storage_key)

Schritt 2: Datei in MinIO hochladen

Schritt 3: Wenn Upload erfolgreich -> Datensatz in Postgres committen

## TODO:
- [ ] add an overview and plan of the architechture to the readme
- [ ] add proper documentation for all the different subcomponents 
- [ ] add todos for all the subcomponents
- [ ] figure out how ot manage my redis queues especially the url queue and if it dedupes or no
- [ ] set up the dispatcher to control the redis queue
- [ ] make the dispatcher frontend to control how many workers we start, what concurrency and rate limiting they have, what domain we want in the scrape filter
