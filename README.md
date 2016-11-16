# shavadoop

* La description des classes et méthodes est disponible dans la JavaDoc jointe.
Schématiquement, le Master peut être appelé dans  Eclipse ou via un runnable JAR en ligne de commande (java -jar Master.jar). 
Celui-ci s'occupe de trouver les hosts disponibles sur le réseau via un script Python, splitter un fichier passé en entrée 
(code du travail par exemple) par ligne (ou blocs de lignes)  et d'écrire chaque split dans un fichier Sx. 
Il distribue ensuite les données (splits) et les calculs (Map) sur le réseau via des Threads qui appellent en SSH la méthode map
contenue dans le Slave qui doit être exporté en JAR exécutable.

* Le Slave effectue le mapping (wordcount) sur les splits qu'il a reçu et écrit sur la sortie standard
les paires <clés,Valeurs> qu'il a calculé (La clé est un mot, la valeur est le compte du mot dans le split). 
Le Master récupère ensuite l'ensemble des paires clés valeurs puis redistribue ces paires (shuffling) sur les hosts disponibles
pour effectuer le reduce. Pour ce faire des Threads sont créés par le Master avec en entrée des paires clés valeurs à réduire.
Les Threads appellent la méthode Reduce du Slave en SSH qui vont ensuite effectuer 
le reduce et écrire le résultat sur la sortie standard.
Le Master va récupérer les résultats et les agréger (assembling) dans le fichier final.

__La documentation complète est disponible dans Shavadoop_Report.odt__
