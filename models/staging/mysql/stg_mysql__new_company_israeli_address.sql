{{ config(
    materialized='table',
    schema=var('mysql_staging_schema'),
    tags=['product_analytics', 'ecosystem']
) }}

WITH source AS (
    SELECT * FROM {{ source('mysql', 'New_Company_israeli_address') }}
),

base AS (
    SELECT
        * EXCEPT(israeli_address_city),
        CASE 
            WHEN NOT REGEXP_CONTAINS(israeli_address_city, r'^[a-zA-Z]{2}') THEN NULL 
            ELSE israeli_address_city 
        END AS israeli_address_city
    FROM source
),

address_fixes AS (
    SELECT
        id ,
        israeli_address_geo_city_key,
        israeli_address_registrar_id,
        origin_entity_id as entity_id ,
        israeli_address_not_active,
        israeli_address_city,
        israeli_address_registrar_name,
        israeli_address_first_rd_center,
        israeli_address_opened_date,
        REPLACE(REPLACE(REPLACE(REPLACE(israeli_address_office_type, '[', ''), ']', ''), '"', ''), ',', ', ') AS israeli_address_office_type,
        israeli_address_address,
        CASE
            WHEN israeli_address_city = 'Beersheba' THEN "Be''er Sheva"
            WHEN israeli_address_city = 'Bet Shemesh' THEN 'Beit Shemesh'
            WHEN israeli_address_city = "Beit Yitshak Sha''ar Hefer" THEN "Beit Yitzhak-Sha''ar Hefer"
            WHEN israeli_address_city = 'Caesarea Industrial Park' THEN 'Caesarea'
            WHEN israeli_address_city = "Giv''at Shmu''el" THEN "Giv''at Shmuel"
            WHEN israeli_address_city = 'Hertsliya' THEN 'Herzliya'
            WHEN israeli_address_city = 'Herzliya Pituach' THEN 'Herzliya'
            WHEN israeli_address_city = 'Kefar Sava' THEN 'Kfar Saba'
            WHEN israeli_address_city = 'Kiryat Motskin' THEN 'Kiryat Motzkin'
            WHEN israeli_address_city = "Kohav Ya''ir" THEN "Kokhav Ya''ir Tzur Yigal"
            WHEN israeli_address_city = "Modi''in Makabim-Re''ut" THEN "Modi''in-Maccabim-Re''ut"
            WHEN israeli_address_city = 'Nahariya' THEN 'Nahariyya'
            WHEN israeli_address_city = 'Kfar Qasim' THEN 'Kafr Qasim'
            WHEN israeli_address_city = 'Kibbutz Ruhama' THEN 'Ruhama'
            WHEN israeli_address_city = 'Natsrat Ilit' THEN 'Nazareth Iliit'
            WHEN israeli_address_city = 'Pardes Hana-Karkur' THEN 'Pardes Hanna-Karkur'
            WHEN israeli_address_city = 'Yahud Monoson' THEN 'Yehud'
            WHEN israeli_address_city = "Zihron Ya''akov" THEN "Zikhron Ya''akov"
            WHEN israeli_address_city = 'Tsofit' THEN 'Tzofit'
            WHEN israeli_address_city = "Ma''alot Tarshiha" THEN "Ma''alot-Tarshiha"
            WHEN israeli_address_city IS NULL OR israeli_address_city = '' THEN
                CASE
                    WHEN israeli_address_address LIKE '%Misgav%' THEN 'Misgav Regional Council'
                    WHEN israeli_address_address LIKE '%Beit Shemesh%' OR israeli_address_address LIKE '%Bet Shemesh%' THEN 'Beit Shemesh'
                    WHEN israeli_address_address LIKE '%Kafr Qasim%' THEN 'Kafr Qasim'
                    WHEN israeli_address_address LIKE '%Rotem Industrial Park%' THEN 'Rotem Industrial Park'
                    WHEN israeli_address_address LIKE '%Tel Aviv%' THEN 'Tel Aviv-Yafo'
                    WHEN israeli_address_address LIKE '%Airport City%' THEN 'Airport City'
                    WHEN israeli_address_address LIKE '%Emek Hefer%' THEN 'Emek Hefer Industrial Park'
                    WHEN israeli_address_address LIKE '%Jerusalem%' THEN 'Jerusalem'
                    WHEN israeli_address_address LIKE '%Beer Tuvya%' THEN "Be''er Tuvia"
                    WHEN israeli_address_address LIKE '%Tefen%' THEN 'Tefen'
                    WHEN israeli_address_address LIKE '%Kfar Ezion%' THEN 'Kfar Ezion'
                    WHEN israeli_address_address LIKE '%Sderot, Israel%' THEN 'Sderot'
                    WHEN israeli_address_address LIKE '%Industrial Park Kidmat%' THEN 'Tiberias'
                    WHEN israeli_address_address LIKE '%Kiryat Arye%' THEN 'Petah Tikva'
                    WHEN israeli_address_address LIKE '%Afula%' THEN 'Afula'
                    WHEN israeli_address_address LIKE '%Kibbutz Sasa%' THEN 'Sasa'
                    WHEN israeli_address_address LIKE '%Hof HaCarmel%' THEN "Ma''ayan Tzvi"
                    WHEN israeli_address_address LIKE '%gadera%' THEN 'Gedera'
                    WHEN israeli_address_address LIKE '%Kerem Maharal%' THEN 'Kerem Maharal'
                    WHEN israeli_address_address LIKE '%Netanya%' THEN 'Netanya'
                    WHEN israeli_address_address LIKE '%Haifa%' THEN 'Haifa'
                    WHEN israeli_address_address LIKE '%Kfar Saba%' THEN 'Kfar Saba'
                    WHEN israeli_address_address LIKE "%Be''er Sheva%" THEN "Be''er Sheva"
                    ELSE israeli_address_city
                END
            ELSE israeli_address_city
        END AS israeli_address_fixed_city
    FROM base
),

classified_addresses AS (
    SELECT
        *,
        CASE
            WHEN israeli_address_city IN ('Ashdod', 'Ashkelon', 'Kiryat Gat', 'Sderot', 'Kiryat Malakhi', 'Revadim', 'Timorim', 'Bror Hayil', 'Kibbutz Ruhama', 'Nir Am', 'Be\'er Tuvia', 'Hatsor Ashdod', 'Kfar Menahem', 'Nir Banim', 'Yad Mordechai', 'Be\'er Tuvia Regional Council', 'Beit Shikma', 'Dorot', 'Ein Tzurim', 'Eliav', 'Emunim', 'Erez', 'Gilon', 'Gvar\'am', 'Hatsav', 'Kfar Aza', 'Lachish', 'Masu\'ot Itzhak', 'Negba', 'Netiv HaAsara', 'Or HaNer', 'Sde Yoav', 'Shekef', 'Shtulim', 'Yad Natan', 'Zikim') THEN 'Ashkelon'
            WHEN israeli_address_city IN ('Caesarea', 'Zikhron Ya\'akov', 'Binyamina-Giv\'at Ada', 'Or Akiva', 'Pardes Hanna-Karkur', 'Hadera', 'Atlit', 'Baqa al-Gharbiyye', 'Gan Shmuel', 'HaHotrim', 'Ma\'agan Michael', 'Ma\'anit', 'Bat Shlomo', 'Ein Carmel', 'Ma\'ayan Tzvi', 'Maor', 'Menashe Regional Council', 'Nahsholim', 'Amikam', 'Barkai', 'Beit Oren', 'Dor', 'Ein Ayala', 'Ein Hod', 'Ein Shemer', 'HaBonim', 'Kafr Qara', 'Ma\'ale Iron', 'Magal', 'Metzer', 'Sde Yitzhak') THEN 'Hadera'
            WHEN israeli_address_city IN ('Haifa', 'Tirat Carmel', 'Nesher', 'Kiryat Tiv\'on', 'Kiryat Bialik', 'Kiryat Ata', 'Yagur', 'Kiryat Motzkin', 'Alonim', 'Kiryat Yam', 'Nofit', 'Ramat Yohanan', 'Daliyat al-Karmel', 'Isfiya', 'Kfar Hasidim', 'Sha\'ar HaAmakim') THEN 'Haifa'
            WHEN israeli_address_city IN ('Ra\'anana', 'Netanya', 'Kfar Saba', 'Hod Hasharon', 'Even Yehuda', 'Kadima Zoran', 'Kokhav Ya\'ir Tzur Yigal', 'Kfar Netter', 'Ramot HaShavim', 'Shefayim', 'Rishpon', 'Tel Mond', 'Kfar Monash', 'Yakum', 'Emek Hefer Industrial Park', 'Kfar Vitkin', 'Yarkona', 'Beit Yanai', 'Givat Hen', 'Neve Yarak', 'Pardesiya', 'Beit Herut', 'Beit Yitzhak-Sha\'ar Hefer', 'Ein Vered', 'Einat', 'Eyal', 'Kfar Malal', 'Mikhmoret', 'Neve Yamin', 'Sde Warburg', 'Udim', 'Adanim', 'Batzra', 'Bnei Dror', 'Bnei Zion', 'Ga\'ash', 'HaOgen', 'Hofit', 'Kfar Hess', 'Tzur Yitzhak', 'Avihayil', 'Beit HaLevi', 'Beit Yehoshua', 'Bitan Aharon', 'Ein HaHoresh', 'Elyakhin', 'Gan Yoshiya', 'Givat Haim', 'Hadar Am', 'Herev Le\'et', 'Herut', 'Horshim', 'Ma\'abarot', 'Nordia', 'Olesh', 'Porat', 'Tnuvot', 'Tzoran-Kadima', 'Tzur Moshe', 'Be\'erotayim', 'Beit Berl', 'Gan Haim', 'Ganei Am', 'Givat Shapira', 'Hagor', 'Haniel', 'Harutzim', 'Havatselet HaSharon', 'Hibat Tzion', 'Hogla', 'Kfar Haroeh', 'Kfar Yona', 'Matan', 'Nahshonim', 'Nirit', 'Nitzanei Oz', 'Ometz', 'Ramat HaKovesh', 'Tayibe', 'Tel Yitzhak', 'Tzofit', 'Yad Hana', 'Yarhiv', 'Herzliya', 'Ramat Hasharon', 'Glil Yam', 'Kfar Shmaryahu') THEN 'HaSharon'
            WHEN israeli_address_city IN ('Jerusalem', 'Beit Shemesh', 'Neve Ilan', 'Mevaseret Zion', 'Shoresh', 'Tzur Hadassah', 'Beit Zait', 'Tal Shahar', 'Tsor\'a', 'Aderet', 'Motza Illit', 'Nataf', 'Ora', 'Tzova', 'Abu Ghosh', 'Aminadav', 'Eshtaol', 'Kfar Uria', 'Kiryat Anavim', 'Kiryat Ye\'arim', 'Luzit', 'Mata', 'Naham', 'Nahshon', 'Nes Harim', 'Neve Shalom', 'Sho\'eva') THEN 'Jerusalem'
            WHEN israeli_address_city IN ('Oranit', 'Ari\'el', 'Ma\'ale Adumim', 'Ramallah', 'Modi\'in Ilit', 'Har Adar', 'Kfar Ezion', 'Kiryat Arba', 'Efrat', 'Ofra', 'Alfei Menashe', 'Alon Shvut', 'Beit Aryeh-Ofarim', 'Beit Horon', 'Betar Illit', 'Elkana', 'Eshkolot', 'Geva Binyamin', 'Hashmona\'im', 'Hinanit', 'Kalya', 'Karnei Shomron', 'Kdumim', 'Kfar Adumim', 'Ma\'ale Shomron', 'Mitzpe Yeriho', 'Neve Daniel', 'Nokdim', 'Shadmot Mehola', 'Yakir', 'Zufim') THEN 'Judea and Samaria'
            WHEN israeli_address_city IN ('Yokne\'am Illit', 'Misgav Regional Council', 'Nazareth', 'Migdal HaEmek', 'Karmiel', 'Afula', 'Nazareth Iliit', 'Ramat Yishai', 'Ma\'alot-Tarshiha', 'Nahariyya', 'Qatsrin', 'Yokne\'am Moshava', 'Rosh Pinna', 'Acre', 'Qiryat Shemona', 'Tefen', 'Tiberias', 'Ein Harod', 'Kfar Vradim', 'Shlomi', 'Afikim', 'Tsipori', 'Beit She\'an', 'Degania Bet', 'Ein HaShofet', 'Hanita', 'Kfar Yehoshua', 'Megiddo', 'Ramot Menashe', 'Timrat', 'Amirim', 'Ashdot Ya\'akov Ihud', 'Atsmon Segev', 'Beit Alfa', 'Bethlehem of Galilee', 'Degania Alef', 'Eilon', 'Ein HaMifratz', 'Ein HaNatziv', 'Ginosar', 'Givat Ela', 'Givat Oz', 'Gvat', 'Hanaton', 'Har Halutz', 'HaZore\'a', 'Kfar Haruv', 'Kfar Masaryk', 'Mahanayim', 'Manof', 'Mitzpe Netofa', 'Nahalal', 'Ram-On', 'Regba', 'Sasa', 'Shamir', 'Sharona', 'Shefa-\'Amr', 'Tel Adashim', 'Alumot', 'Ami\'ad', 'Amir', 'Ani\'am', 'Basmet Tab\'un', 'Beit HaEmek', 'Beit Jann', 'Beit Keshet', 'Beit Zera', 'Dafna', 'Dalia', 'Dvora', 'Ein Dor', 'Ein Gev', 'Eshhar', 'Evron', 'Ga\'aton', 'Gadot', 'Gal\'ed', 'Gan Ner', 'Gazit', 'Gesher', 'Gesher HaZiv', 'Geva', 'Ginegar', 'Gonen', 'Goren', 'Heftziba', 'Hila', 'Kabri', 'Kadarim', 'Kafr Kanna', 'Kafr Yasif', 'Kfar Baruch', 'Kfar Blum', 'Kfar Giladi', 'Kfar HaNassi', 'Kfar Ruppin', 'Kfar Tavor', 'Klil', 'Koranit', 'Lavi', 'Lavon', 'Lehavot HaBashan', 'Lehavot Haviva', 'Livnim', 'Lotem', 'Majd al-Krum', 'Matzuva', 'Merhavia', 'Merom Golan', 'Meron', 'Mevo Hama', 'Misgav', 'Misgav Am', 'Mishmar HaEmek', 'Mitzpa', 'Mitzpe Aviv', 'Moran', 'Neve Ativ', 'Neve Eitan', 'Neve Ur', 'Oshrat', 'Ramat Tzvi', 'Reshafim', 'Rosh HaNikra', 'Safed', 'Sde Eliyahu', 'Sde Nehemia', 'Sde Ya\'aqov', 'Sha\'ar HaGolan', 'Shavei Tzion', 'Shomrat', 'Shtula', 'Tsfat', 'Tuval', 'Tzurit', 'Ya\'ad', 'Yarka', 'Yavne\'el', 'Yifat', 'Yizre\'el', 'Yodfat') THEN 'Northern Region'
            WHEN israeli_address_city IN ('Petah Tikva', 'Rehovot', 'Rishon LeTsiyon', 'Rosh Haayin', 'Ness Ziona', 'Modi\'in-Maccabim-Re\'ut', 'Yavne', 'Lod', 'Yehud', 'Giv\'at Shmuel', 'Airport City', 'Shoham', 'Ganei Tikva', 'Savyon', 'Beit Dagan', 'Gedera', 'Be\'er Ya\'akov', 'Bnei Atarot', 'Karmei Yosef', 'Mazor', 'Na\'an', 'Gan Yavne', 'Givat Brenner', 'Magshimim', 'Shilat', 'El\'ad', 'Kfar Truman', 'Mazkeret Batya', 'Kafr Qasim', 'Kfar HaNagid', 'Kfar Rut', 'Mishmar HaShiv\'a', 'Nir Tzvi', 'Ramla', 'Be\'erot Yitzhak', 'Beit Gamliel', 'Beit Hanan', 'Beit Hashmonai', 'Beit Nehemia', 'Ben-Gurion-Airport', 'Bnei Darom', 'Givat HaShlosha', 'Kfar Sirkin', 'Nehalim', 'Nir Galim', 'Beit Hilkia', 'Bnei Ayish', 'Ganei Tal', 'Gezer', 'Giv\'at Ko\'ah', 'Hadid', 'Hafetz Haim', 'Hevel Modiin Industrial Park', 'Kfar Aviv', 'Kfar Bilu', 'Kfar Bin Nun', 'Kfar Mordechai', 'Kfar Qasim', 'Kidron', 'Kiryat Ekron', 'Kvutzat Yavne', 'Meishar', 'Nofekh', 'Palmachim', 'Ramot Meir', 'Rinatya', 'Shdema', 'Yad Rambam') THEN 'Shfela'
            WHEN israeli_address_city IN ('Be\'er Sheva', 'Omer', 'Eilat', 'Ofakim', 'Lehavim', 'Dimona', 'Eilot', 'Yeruham', 'Ketura', 'Netivot', 'Revivim', 'Arad', 'Dvir', 'Hura', 'Magen', 'Nir Yitzhak', 'Paran', 'Rotem Industrial Park', 'Yotvata', 'Alumim', 'Ashalim', 'Beit HaGadi', 'Beit Kama', 'Ein HaBesor', 'Hatzerim', 'Kfar Maimon', 'Lotan', 'Mashabei Sadeh', 'Meitar', 'Mivtahim', 'Neot HaKikar', 'Ramat Hovav', 'Samar', 'Sde Nitzan', 'Sdei Avraham', 'Sharsheret', 'Shoval', 'Tifrah', 'Tkuma', 'Urim') THEN 'Southern Region'
            WHEN israeli_address_city IN ('Tel Aviv-Yafo', 'Ramat Gan', 'Bnei Brak', 'Holon', 'Or Yehuda', 'Giv\'atayim', 'Kiryat Ono', 'Azor', 'Bat Yam') THEN 'Tel Aviv-Yafo'
            ELSE NULL
        END AS israeli_address_metropolis,
        CASE
            WHEN israeli_address_fixed_city = 'Tel Aviv-Yafo' THEN 'Tel Aviv-Yafo'
            WHEN israeli_address_fixed_city IN ('Jerusalem', 'Ma\'ale Adumim') THEN 'Jerusalem'
            WHEN israeli_address_fixed_city IN ('Haifa', 'Nesher', 'Tirat Carmel') THEN 'Haifa'
            WHEN israeli_address_fixed_city = 'Petah Tikva' THEN 'Petah Tikva'
            WHEN israeli_address_fixed_city IN ('Ra\'anana', 'Kfar Saba') THEN 'Ra\'anana - Kfar Saba'
            WHEN israeli_address_fixed_city = 'Netanya' THEN 'Netanya'
            WHEN israeli_address_fixed_city = 'Rehovot' THEN 'Rehovot'
            WHEN israeli_address_fixed_city = 'Be\'er Sheva' THEN 'Be\'er Sheva'
            WHEN israeli_address_fixed_city = 'Herzliya' THEN 'Herzliya'
            WHEN israeli_address_fixed_city = 'Ashdod' THEN 'Ashdod'
            WHEN israeli_address_fixed_city IN ('Yokne\'am Moshava', 'Yokne\'am Illit') THEN 'Yokne\'am'
            WHEN israeli_address_fixed_city IN ('Nazareth', 'Nazareth Iliit') THEN 'Nazareth'
            ELSE NULL
        END AS israeli_address_large_city,
        CASE
    -- Tel Aviv District (מחוז תל אביב)
    WHEN israeli_address_fixed_city IN (
        'Tel Aviv-Yafo', 'Tel Aviv-Jaffa', 'Ramat Gan', 'Bnei Brak', 'Holon', 
        'Giv\'atayim', 'Kiryat Ono', 'Bat Yam', 'Or Yehuda', 'Azor',
        'Ramat HaSharon', 'Ramat Hasharon', 'Herzliya', 'Givatayim'
    ) THEN 'Tel Aviv District'
    
    -- Central District (מחוז מרכז)
    WHEN israeli_address_fixed_city IN (
        'Petah Tikva', 'Rishon LeTsiyon', 'Rishon LeZion', 'Netanya', 'Rehovot',
        'Kfar Saba', 'Ra\'anana', 'Hod HaSharon', 'Hod Hasharon', 'Lod', 'Ramla',
        'Ness Ziona', 'Yavne', 'Rosh Haayin', 'Modi\'in-Maccabim-Re\'ut', 
        'Modi\'in Makabim-Re\'ut', 'Giv\'at Shmuel', 'Giv\'at Shmu\'el',
        'Ganei Tikva', 'Yehud-Monosson', 'Yehud', 'Shoham', 'Gedera',
        'Be\'er Ya\'akov', 'Airport City', 'El\'ad', 'Even Yehuda', 
        'Kadima Zoran', 'Kadima Tzoran', 'Kokhav Ya\'ir Tzur Yigal',
        'Kfar Yona', 'Tel Mond', 'Savyon', 'Beit Dagan', 'Kafr Qasim',
        'Kfar Qasim', 'Gan Yavne', 'Kiryat Ekron', 'Mazkeret Batya',
        'Bnei Atarot', 'Karmei Yosef', 'Mazor', 'Na\'an', 'Givat Brenner',
        'Magshimim', 'Shilat', 'Kfar Truman', 'Kfar HaNagid', 'Kfar Rut',
        'Mishmar HaShiv\'a', 'Nir Tzvi', 'Be\'erot Yitzhak', 'Beit Gamliel',
        'Beit Hanan', 'Beit Hashmonai', 'Beit Nehemia', 'Ben-Gurion-Airport',
        'Givat HaShlosha', 'Kfar Sirkin', 'Nehalim', 'Nir Galim', 'Beit Hilkia',
        'Bnei Ayish', 'Ganei Tal', 'Gezer', 'Giv\'at Ko\'ah', 'Hadid',
        'Kfar Aviv', 'Kfar Bin Nun', 'Kfar Mordechai', 'Kidron', 'Kvutzat Yavne',
        'Meishar', 'Nofekh', 'Palmachim', 'Ramot Meir', 'Rinatya', 'Shdema',
        'Yad Rambam', 'Kfar Netter', 'Ramot HaShavim', 'Shefayim', 'Rishpon',
        'Kfar Monash', 'Yakum', 'Emek Hefer Industrial Park', 'Kfar Vitkin',
        'Yarkona', 'Beit Yanai', 'Givat Hen', 'Neve Yarak', 'Pardesiya',
        'Beit Herut', 'Beit Yitzhak-Sha\'ar Hefer', 'Ein Vered', 'Einat',
        'Eyal', 'Kfar Malal', 'Mikhmoret', 'Neve Yamin', 'Sde Warburg', 'Udim',
        'Adanim', 'Batzra', 'Bnei Dror', 'Bnei Zion', 'Ga\'ash', 'HaOgen',
        'Hofit', 'Kfar Hess', 'Tzur Yitzhak', 'Beit HaLevi', 'Beit Yehoshua',
        'Ein HaHoresh', 'Gan Yoshiya', 'Givat Haim', 'Hadar Am', 'Herev Le\'et',
        'Herut', 'Horshim', 'Ma\'abarot', 'Olesh', 'Porat', 'Tnuvot',
        'Tzur Moshe', 'Be\'erotayim', 'Beit Berl', 'Givat Shapira', 'Haniel',
        'Harutzim', 'Havatselet HaSharon', 'Hibat Tzion', 'Hogla', 'Kfar Haroeh',
        'Matan', 'Nahshonim', 'Nirit', 'Ometz', 'Ramat HaKovesh', 'Tel Yitzhak',
        'Tzofit', 'Yad Hana', 'Yarhiv', 'Glil Yam', 'Kfar Shmaryahu',
        'Beit Elazari', 'Beit Uziel', 'Netzer Sereni', 'Pedaya', 'Yashresh',
        'Beit Nekofa', 'Lapid', 'Nir Hen', 'Sdei Hemed', 'Bat Hefer',
        'Nordia', 'Bareket', 'Gan HaShomron', 'Ganot Hadar', 'Kfar Yehezkel',
        'Burgata', 'Gan Sorek', 'Beit Zayit', 'Avigdor', 'Nitzanim',
        'Nahshon', 'Kvutzat Shiller', 'Bnei Darom', 'Luzit', 'Tzrufa',
        'Ben Shemen', 'Kfar Habad', 'Tirat Yehuda', 'Kfar Ma\'as',
        'Azri\'el', 'Na\'ale', 'Nof Ayalon', 'Kfar Warburg', 'Mesilat Zion',
        'Hulda'
    ) THEN 'Central District'
    
    -- Haifa District (מחוז חיפה)
    WHEN israeli_address_fixed_city IN (
        'Haifa', 'Hadera', 'Tirat Carmel', 'Nesher', 'Kiryat Bialik',
        'Kiryat Ata', 'Kiryat Motzkin', 'Kiryat Yam', 'Kiryat Tiv\'on',
        'Yokne\'am Illit', 'Yokne\'am Moshava', 'Or Akiva', 
        'Pardes Hanna-Karkur', 'Caesarea', 'Zikhron Ya\'akov', 'Zihron Ya\'akov',
        'Binyamina-Giv\'at Ada', 'Atlit', 'Baqa al-Gharbiyye', 'Fureidis',
        'Yagur', 'Alonim', 'Nofit', 'Daliyat al-Karmel', 'Isfiya',
        'Sha\'ar HaAmakim', 'Ramat Yohanan', 'Ein Carmel', 'Ein Hod',
        'Ma\'ayan Tzvi', 'Ma\'anit', 'Bat Shlomo', 'Gan Shmuel', 'HaHotrim',
        'Ma\'agan Michael', 'Maor', 'Menashe Regional Council', 'Nahsholim',
        'Amikam', 'Barkai', 'Beit Oren', 'Dor', 'Ein Shemer', 'HaBonim',
        'Kafr Qara', 'Ma\'ale Iron', 'Magal', 'Metzer', 'Sde Yitzhak',
        'Dalia', 'Megiddo', 'Ramot Menashe', 'Ein HaShofet', 'HaZore\'a',
        'Gvat', 'Mishmar HaEmek', 'Ramat HaShofet', 'Gan HaDarom',
        'Mei Ami', 'Gal\'ed', 'Lehavot Haviva', 'Givat Oz', 'Aviel',
        'Carmel'
    ) THEN 'Haifa District'
    
    -- Northern District (מחוז צפון)
    WHEN israeli_address_fixed_city IN (
        'Nazareth', 'Nof HaGalil', 'Nazareth Iliit', 'Tiberias', 'Safed',
        'Acre', 'Nahariyya', 'Karmiel', 'Afula', 'Migdal HaEmek',
        'Ma\'alot-Tarshiha', 'Qiryat Shemona', 'Kiryat Shmona', 'Beit She\'an',
        'Rosh Pinna', 'Qatsrin', 'Tefen', 'Migdal Tefen', 'Shlomi',
        'Kfar Vradim', 'Ramat Yishai', 'Timrat', 'Afikim', 'Tsipori', 'Tzippori',
        'Shefa-Amr', 'Shefa-\'Amr', 'Tamra', 'Sakhnin', 'Ar\'ara',
        'Kafr Kanna', 'Kafr Yasif', 'Yarka', 'Majd al-Krum', 'Tira',
        'Misgav Regional Council', 'Misgav', 'Ein Harod', 'Kfar Yehoshua',
        'Hanita', 'Degania Bet', 'Degania Alef', 'Amirim', 'Ashdot Ya\'akov Ihud',
        'Atsmon Segev', 'Beit Alfa', 'Bethlehem of Galilee', 'Eilon',
        'Ein HaMifratz', 'Ein HaNatziv', 'Ginosar', 'Givat Ela', 'Hanaton',
        'Har Halutz', 'Kfar Haruv', 'Kfar Masaryk', 'Mahanayim', 'Manof',
        'Mitzpe Netofa', 'Nahalal', 'Ram-On', 'Regba', 'Sasa', 'Shamir',
        'Sharona', 'Tel Adashim', 'Alumot', 'Ami\'ad', 'Amir', 'Ani\'am',
        'Basmet Tab\'un', 'Beit HaEmek', 'Beit Jann', 'Beit Keshet',
        'Beit Zera', 'Dafna', 'Dvora', 'Ein Dor', 'Ein Gev', 'Eshhar',
        'Evron', 'Ga\'aton', 'Gadot', 'Gan Ner', 'Gazit', 'Gesher HaZiv',
        'Geva', 'Ginegar', 'Gonen', 'Goren', 'Heftziba', 'Kabri', 'Kadarim',
        'Kfar Blum', 'Kfar HaNassi', 'Kfar Ruppin', 'Kfar Tavor', 'Klil',
        'Koranit', 'Lavi', 'Lavon', 'Lehavot HaBashan', 'Lotem', 'Matzuva',
        'Merhavia', 'Merom Golan', 'Meron', 'Mevo Hama', 'Misgav Am',
        'Mitzpa', 'Mitzpe Aviv', 'Moran', 'Neve Eitan', 'Neve Ur',
        'Ramat Tzvi', 'Reshafim', 'Rosh HaNikra', 'Sde Eliyahu', 'Sde Nehemia',
        'Sde Ya\'aqov', 'Sha\'ar HaGolan', 'Shomrat', 'Shtula', 'Tuval',
        'Tzurit', 'Ya\'ad', 'Yavne\'el', 'Yifat', 'Yizre\'el', 'Yodfat',
        'Chorazim', 'Almagor', 'Alon HaGalil', 'Alonei Abba', 'Ayelet HaShahar',
        'Bar\'am', 'Dan', 'Ein Ya\'akov', 'Ein Zivan', 'Elifelet', 'Gshur',
        'HaGoshrim', 'HaYogev', 'HaZor\'im', 'Hararit', 'Hatzor HaGlilit',
        'Hulata', 'Kaduri Regional Center', 'Kfar HaHoresh', 'Kfar HaRif',
        'Maoz Haim', 'Mattat', 'Metula', 'Mikhmanim', 'Mishmar HaNegev',
        'Mitzpe Ilan', 'Mizra', 'Nir Yisrael', 'Peki\'in', 'Poria Illit',
        'Sde Nahum', 'Sdot Yam', 'Shimshit', 'Shorashim', 'Tel Yosef',
        'Yesud HaMa\'ala', 'Avnei Eitan', 'Bustan HaGalil', 'Ein HaEmek',
        'Hoshaya', 'Neve Ziv', 'Sarid', 'Tal-El', 'Ahihud',
        'Dekel', 'Gilon', 'Yas\'ur'
    ) THEN 'Northern District'
    
    -- Jerusalem District (מחוז ירושלים)
    WHEN israeli_address_fixed_city IN (
        'Jerusalem', 'Beit Shemesh', 'Mevaseret Zion', 'Tzur Hadassah',
        'Neve Ilan', 'Shoresh', 'Beit Zait', 'Tal Shahar', 'Tsor\'a',
        'Aderet', 'Motza Illit', 'Nataf', 'Ora', 'Tzova', 'Aminadav',
        'Eshtaol', 'Kfar Uria', 'Kiryat Anavim', 'Kiryat Ye\'arim', 'Mata',
        'Naham', 'Nes Harim', 'Sho\'eva', 'Givat Yeshayahu', 'Even Shmuel',
        'Beit Nir', 'Har Adar', 'Mevo Beitar', 'Mevo Horon', 'Giv\'at Ye\'arim',
        'Ramat Raziel', 'Tirosh'
    ) THEN 'Jerusalem District'
    
    -- Southern District (מחוז דרום)
    WHEN israeli_address_fixed_city IN (
        'Be\'er Sheva', 'Ashdod', 'Ashkelon', 'Eilat', 'Dimona', 'Arad',
        'Sderot', 'Kiryat Gat', 'Kiryat Malakhi', 'Ofakim', 'Netivot',
        'Yeruham', 'Rahat', 'Omer', 'Lehavim', 'Meitar', 'Be\'er Tuvia',
        'Hatsor Ashdod', 'Eilot', 'Ketura', 'Revivim', 'Magen', 'Nir Yitzhak',
        'Paran', 'Yotvata', 'Alumim', 'Ashalim', 'Beit Kama', 'Ein HaBesor',
        'Hatzerim', 'Kfar Maimon', 'Lotan', 'Mashabei Sadeh', 'Neot HaKikar',
        'Ramat Hovav', 'Samar', 'Sde Nitzan', 'Sdei Avraham', 'Sharsheret',
        'Shoval', 'Urim', 'Dvir', 'Hura', 'Nir Am', 'Revadim', 'Timorim',
        'Bror Hayil', 'Kfar Menahem', 'Nir Banim', 'Yad Mordechai', 'Dorot',
        'Ein Tzurim', 'Eliav', 'Emunim', 'Erez', 'Gvar\'am', 'Hatsav',
        'Kfar Aza', 'Lachish', 'Masu\'ot Itzhak', 'Negba', 'Netiv HaAsara',
        'Or HaNer', 'Sde Yoav', 'Shtulim', 'Yad Natan', 'Zikim', 'Gevim',
        'Nir Oz', 'Nirim', 'Re\'im', 'Sufa', 'Sa\'ad', 'Lahav', 'Sde Boker',
        'Sde David', 'Neot Semadar', 'Tze\'elim', 'Ruhama', 'Arugot',
        'Aseret', 'Nehora', 'Nitzanim', 'Tzofar'
    ) THEN 'Southern District'
    
    ELSE NULL
END AS israeli_address_district
    FROM address_fixes
)

SELECT
    id,
    entity_id,
    israeli_address_geo_city_key,
    israeli_address_registrar_id as registrar_id,
    israeli_address_not_active,
    israeli_address_city,
    israeli_address_registrar_name,
    israeli_address_first_rd_center,
    israeli_address_opened_date,
    israeli_address_office_type,
    israeli_address_address,
    israeli_address_fixed_city,
    israeli_address_metropolis,
    israeli_address_large_city,
    israeli_address_district
FROM classified_addresses