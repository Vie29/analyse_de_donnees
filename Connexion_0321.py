import mysql.connector as mysql

class Connexion:
    __host = 'localhost'
    __port = '8081'
    __database = 'eminem'
    __user = 'root'
    __password = 'root'

    __curseur = None

#methode de connexion a la bdd
    @classmethod
    def ouvrir_connexion(cls):
        if cls.__curseur == None :
            cls.__bdd = mysql.connect(user = cls.__user,
                                    password = cls.__password,
                                    host = cls.__host,
                                    port = cls.__port,
                                    database = cls.__database)
            cls.__curseur = cls.__bdd.cursor(buffered=True)

#methode pour fermer la connexion a la bdd
    @classmethod
    def fermer_connexion(cls):
        cls.__curseur.close()
        cls.__bdd.close()
        cls.__curseur = None

#methodes pour alimenter la base SQL à partir du fichier CSV importe
    @classmethod
    def remplir_table_album(cls, data):
        liste_album=[]
        cls.ouvrir_connexion()
        #Pour eviter d avoir plusieurs Album_id pour le meme nom d album 
        for i in data.index :
            album_name = data['Album_Name'][i]
            album_url = data['Album_URL'][i]
            if album_name in liste_album:
                continue
            liste_album.append(album_name)    
            remplir_table_album = "INSERT INTO album (Album_name, Album_url) VALUES (%s, %s)"
            val = (album_name, album_url) 
            cls.__curseur.execute(remplir_table_album, val)
            cls.__bdd.commit()
        cls.fermer_connexion()    

    @classmethod
    def remplir_table_song(cls, data):
        cls.ouvrir_connexion()
        for i in data.index :
            song_name = data['Song_Name'][i]
            lyrics = data['Lyrics'][i]
            views = data['Views'][i]
            release_date = data['Release_date'][i]
            #Pour faire correspondre a une chanson l album correpondant         
            album_id_join_song = (f'''SELECT Album_id 
                                      FROM album where Album_name = \"{data['Album_Name'][i]}\" ;''')
            cls.__curseur.execute(album_id_join_song)
            #Fetchone pour avoir une seule valeur au lieu de fetchall & pour avoir le data et pas TRUE ou FAlSE avec le execute   
            x = cls.__curseur.fetchone()
            album_id = x[0]
            remplir_table_song = "INSERT INTO songs (Songs_name, Lyrics, Views, Album_id, Release_date) VALUES (%s, %s, %s, %s, %s)"
            val = (song_name, lyrics, views, album_id, release_date)                               
            cls.__curseur.execute(remplir_table_song, val)
            cls.__bdd.commit()
        cls.fermer_connexion()       


    @classmethod
    def creer_vue(cls):
        cls.ouvrir_connexion()
        drop = "DROP VIEW IF EXISTS Nb_de_vues;"
        query = f'''CREATE VIEW Nb_de_vues
                    AS SELECT Songs_name, Album_id, Views
                    FROM songs
                    ORDER BY Views DESC;'''
        cls.__curseur.execute(drop)
        cls.__curseur.execute(query)
        cls.__bdd.commit()
        cls.fermer_connexion()
   
    @classmethod
    def creer_procedure(cls):
        cls.ouvrir_connexion()
        drop = "DROP PROCEDURE IF EXISTS localisation_song;"
        query = f'''CREATE PROCEDURE localisation_song 
                    (IN titre CHAR(100))
                    BEGIN SELECT a.Album_Name, s.Songs_name
                    FROM album a
                    JOIN songs s ON a.Album_id = s.Album_id
                    WHERE s.Album_Name = titre;
                    END''' 

        cls.__curseur.execute(drop)
        cls.__curseur.execute(query)
        cls.__bdd.commit()
        cls.fermer_connexion()