<div align = "center">

# **SANDBOXING SCRIPT LF SP2020 BPS PROVINSI SUMATERA SELATAN**

---
<div align = "center">

<img src = "assets/profile.png" width="100" height="100"></img>
### **Ari Rismansyah | @arirismansyah**



© Copyright 2022 - Ari Rismansyah – ari.rismansyah@bps.go.id

---

</div>

</div>


<br>

## **Deskripsi**

<div align = "justify">

Berikut ini merupakan scipt sandbox untuk explorasi dan identifikasi anomali data hasil pendataan LF SP2020 pada wilayah CAPI Provinsi Sumatera Selatan berdasarkan beberapa case yang disusun oleh subjectmatter & tim LF SP2020 BPS Provinsi Sumatera Selatan


</div>

<br>

### 01 - JUMLAH ART R.112 SAMA DENGAN JUMLAH ART BLOK III 
```
%jdbc(hive)
use splf2020_16;

select t1.id, t1.kab_fullcode as kab, t1.blok_sensus_fullcode as bs, t1.nks as nks, t1.nus as nus, t1.jml_art as jumlah_art_112, t2.jumlah_art_blok_iv as jumlah_art_count_blok_iv, t3.no_urut_art_max as blok_iii_no_urut_art_max

from (
    select id, kab_fullcode, nus, nks, blok_sensus_fullcode, cast(jml_art as int)
    from keluarga_16
) t1

left join (
    select id, count (*) as jumlah_art_blok_iv
    from b4_16
    group by id
) t2
on t1.id = t2.id
left join(
    select id, max(rosterindex) as no_urut_art_max
    from b4_16
    group by id
) t3
on t1.id = t3.id
where (t1.jml_art != t2.jumlah_art_blok_iv) or (t1.jml_art != t3.no_urut_art_max)

```

### 02 - PASTIKAN UMUR TERISI & DIANTARA 0-95 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur, current_role as status_assignment
from b4_16 
where (r306 > 95) or (isnull(r306)) or (r306 < 0) 
```

### 03 - KONSISTENSI ISIAN UMUR DENGAN BULAN & TAHUN LAHIR
```
%jdbc(hive)
use splf2020_16;
select * 
from (
    select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur_fasih,  if((r305b>6), (2022 - r305c-1), (2022-r305c)) as umur_calculate, current_role as status_assignment
    from b4_16 
) t1
where (umur_fasih != 95) and (umur_fasih != umur_calculate) and (bln_lahir != 98)
```

### 04 - JENIS KELAMIN HARUS TERISI
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r304 as jenis_kelamin, current_role as status
from b4_16 
where isnull(r304)
```

### 05 - KONSISTENSI JENIS KELAMIN & STATUS HUBUNGAN (1) 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r303 as hubungan_dgn_krt, r304 as jenis_kelamin, current_role as status
from b4_16 
where (r303=2 and r304!=1) or (r303=3 and r304!=2)
```

### 05 - KONSISTENSI JENIS KELAMIN & STATUS HUBUNGAN (2) 
```
%jdbc(hive)
use splf2020_16;

select pasangan_krt.id, pasangan_krt.kab_fullcode as kab, pasangan_krt.blok_sensus_fullcode as bs, pasangan_krt.nus as nus_pasangan, krt.nus as nus_krt, pasangan_krt.rosterIndex as no_urut_art_pasangan, pasangan_krt.r303 as hubungan_pasangan_krt, krt.rosterIndex as no_urut_art_krt, pasangan_krt.r304 as jk_pasangan, krt.r304 as jk_krt

from (
    select id, kab_fullcode, blok_sensus_fullcode, nus, rosterIndex, r303, r304, current_role
    from b4_16
    where r303 = 2 or r303 = 3
) pasangan_krt

left join (
    select id, kab_fullcode, blok_sensus_fullcode, nus, rosterIndex, r303, r304, current_role
    from b4_16
    where r303 = 1
) krt

on pasangan_krt.id = krt.id

where pasangan_krt.r304 = krt.r304
```

### 06 - KOMUTER R.427 = 1, KAB KEGIATAN KOMUTER TIDAK BOLEH SAMA DENGAN TEMPAT TINGGAL 
```
%jdbc(hive)
use splf2020_16;

select kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r427 as status_komuter, r428b as kab_komuter
from b4_16
where (r427 = 1) and (kab_fullcode == r428b)
```
### 07 - LAPANGAN USAHA R.434 & JENIS PEKERJAAN R.435 
```
%jdbc(hive)
use splf2020_16;

SELECT
    id, kab_fullcode as kab, blok_sensus_fullcode AS bs, nus, r301 AS no_urut_art, r432, r433, r434_kode, r435_kode, current_role AS status
FROM
    b4_16
WHERE
    (r432 = 1 OR (r432 =2 AND r433 =1)) AND (r434_kode is null OR r435_kode is null)
```

### 08A - JUMLAH ANAK LAHIR HIDUP R.348 HARUS >= JUMLAH ANAK MASIH HIDUP (R.439+R.440) 
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, kab_fullcode as kab, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_art, r438 AS jumlah_anak_lahir_hidup, (r439a+r439b+r440a+r440b) AS jumlah_anak_masih_hidup, current_role as status
FROM
    b4_16
WHERE 
    r438 < (r439a+r439b+r440a+r440b)
```

### 08B - JUMLAH ANAK LAHIR HIDUP R.438 SAMA DENGAN JUMLAH ANAK MASIH HIDUP R.439, R.440 & JUMLAH ANAK SUDAH MENINGGAL R.441
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, kab_fullcode as kab, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_art, r438 AS jumlah_anak_lahir_hidup, (r439a+r439b+r440a+r440b+r441a+r441b) AS jumlah_anak_seluruhnya, current_role as status
FROM
    b4_16
WHERE 
    r438 != (r439a+r439b+r440a+r440b+r441a+r441b)
```

### 8C - WANITA <= 10 TAHUN, ALH >= 3 
```
%jdbc(hive)

USE splf2020_16;

SELECT
    id, blok_sensus_fullcode AS bs, nus, r301 AS no_urut_art, r306 as umur, r304 as status_kawin, r438 AS ALH, current_role AS status
FROM
    b4_16
WHERE
    r306 <= 10 AND r304 = 2 AND r438 >=3
```

### 09 - KONSISTENSI ALH (JAN 2017) SAMPAI (JAN 2021)
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, kab_fullcode as kab, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_art, r442 AS melahirkan_sejak_jan2017, (r443a+r443b) AS ALH_jan_2017, 
    CASE WHEN (r443a+r443b) <= 0 then 'tidak konsisten' ELSE 'konsisten' END AS flag_ALH_jan_2017, current_role AS status
FROM
    b4_16
WHERE 
    r442=1 AND (r443a+r443b) <= 0
```

### 10 - ANAK (0-14) TAHUN, IBU KANDUNG TINGGAL DI RUMAH, CEK NO URUT IBU KANDUNG, STATUS KAWIN, JK 
```
%jdbc(hive)

--untuk mengecek nomor urut art dan nomor urut induk kandung yang digunakan adalah kolom rosterIndex

USE SPLF2020_16;

SELECT 
    t1.id, t1.BS, t1.nus, t1.no_urut_anak, t1.umur_anak, t1.ibu_kandung_tinggal_dirumah, t1.no_urut_ibu_kandung, t2.no_urut_ibu, t2.jenis_kelamin_ibu, t2.status_kawin_ibu, t1.status
FROM
    (
        SELECT 
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_anak, r306 AS umur_anak, r308a AS ibu_kandung_tinggal_dirumah, r308 AS no_urut_ibu_kandung, current_role AS status
        FROM
            b4_16
        WHERE 
            r308 is not null AND r308a = 1
    )t1,
    (
        SELECT 
            id, blok_sensus_fullcode AS BS, nus, rosterIndex AS no_urut_ibu, r304 AS jenis_kelamin_ibu, r307 AS status_kawin_ibu
        FROM
            b4_16
    )t2
    
WHERE 
    t1.id=t2.id AND (t1.umur_anak between 0 AND 14) AND t1.no_urut_ibu_kandung=t2.no_urut_ibu AND (status_kawin_ibu = 1 OR jenis_kelamin_ibu=1) 

```

### 11 - KONSISTENSI KEMATIAN DI BLOK VII & VI
```
%jdbc(hive)

USE SPLF2020_16;
-- jumlah meninggal akibat kehamilan > jumlah kematian keseluruhan?

SELECT 
    t1.id, t1.BS, t1.nus, t1.jumlah_kematian, t2.jumlah_kematian_kehamilan, t1.status
FROM
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r602 AS jumlah_kematian, current_role AS status
        FROM
            keluarga_16
    )t1 
INNER JOIN
    (
        SELECT
            id, COUNT(*) AS jumlah_kematian_kehamilan
        FROM 
            b7_16
        WHERE
            r703 =2
        GROUP BY 
            id
    )t2
ON 
    t1.id = t2.id AND  t2.jumlah_kematian_kehamilan > t1.jumlah_kematian
```

### 12 - TAHUN MENINGGAL PADA BLOK VII TIDAK DALAM RENTANG (2017-2022) 
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    t1.id, t1.blok_sensus_fullcode AS BS, t1.nus, t2.r301 AS no_urut_art, t1.r704thn, t1.current_role AS status
FROM
    b7_16 t1, b4_16 t2
WHERE
    t1.id = t2.id AND t1.rosterIndex = t2.rosterIndex AND (r704thn < 2017 OR  r704thn > 2022)
```
### 13 - AGAMA ANAK <=10 TAHUN BERBEDA DENGAN ORANG TUA
```
%jdbc(hive)
USE SPLF2020_16;

SELECT
    *
FROM
    (
    SELECT
        t1.id, t1.BS, t1.nus, t1.no_urut_anak, t1.status_anak, t1.agama_anak, t2.no_urut_krt, t2.status_krt, t2.agama_krt, t3.no_urut_pasangan, t3.status_pasangan_krt, t3.agama_pasangan_krt, t1.status_assignment
    FROM
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_anak, r303 AS status_anak, r405 AS agama_anak, current_role AS status_assignment
        FROM
            b4_16
        WHERE
            r303 = 4 AND r306 <= 10
    )t1,
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_krt,r303 AS status_krt, r405 AS agama_krt
        FROM
            b4_16
        WHERE
            r303 = 1
    )t2,
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_pasangan,r303 AS status_pasangan_krt, r405 AS agama_pasangan_krt
        FROM
            b4_16
        WHERE
            r303 = 2 OR r303 = 3
    )t3
    WHERE
        t1.id = t2.id AND t2.id = t3.id 
    ORDER BY 
        t3.agama_pasangan_krt
    )a
WHERE
    agama_anak != agama_pasangan_krt AND agama_anak != agama_krt
    
```
### 14 - SUKU ANAK <= 10 TAHUN BERBEDA DENGAN ORANG TUA 
```
%jdbc(hive)

USE SPLF2020_16;

SELECT
    *
FROM
    (
    SELECT
        t1.id, t1.BS, t1.nus, t1.no_urut_anak, t1.status_anak, t1.suku_anak, t2.no_urut_krt, t2.status_krt, t2.suku_krt, t3.no_urut_pasangan, t3.status_pasangan_krt, t3.suku_pasangan_krt, t1.status_assignment
    FROM
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_anak, r303 AS status_anak, r404 AS suku_anak, current_role AS status_assignment
        FROM
            b4_16
        WHERE
            r303 = 4 AND r306 <= 10
    )t1,
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_krt,r303 AS status_krt, r404 AS suku_krt
        FROM
            b4_16
        WHERE
            r303 = 1
    )t2,
    (
        SELECT
            id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_pasangan,r303 AS status_pasangan_krt, r404 AS suku_pasangan_krt
        FROM
            b4_16
        WHERE
            r303 = 2 OR r303 = 3
    )t3
    WHERE
        t1.id = t2.id AND t2.id = t3.id 
    ORDER BY 
        t3. suku_pasangan_krt
    )a
WHERE
    suku_anak != suku_pasangan_krt AND suku_anak != suku_krt
```
### 15 - ANAK <= 15 TAHUN (R.306) DENGAN STATUS KAWIN (R.307 BERKODE 2, 3, ATAU 4) 14 - SUKU ANAK <= 10 TAHUN BERBEDA DENGAN ORANG TUA 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur, r307 as status_perkawinan
from b4_16 
where r306 <= 15 and (r307 = 2 or r307 = 3 or r307 = 4) 
```
### 16 - HUBUNGAN DENGAN KRT (R.303 : 5,7,8) DENGAN STATUS PERKAWINAN (R.307 : 1) 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r303 as hubungan_dengan_krt, r307 status_perkawinan, current_role as status
from b4_16
where r307 = 1 and (r303 = 5 or r303 = 7 or r303 = 8)
```
### 17 - HUBUNGAN DENGAN KRT (R.303: 2, 3) DENGAN STATUS PERKAWINAN (R.307 KODE != 2) 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r303 as hubungan_dengan_krt, r307 status_perkawinan, current_role as status
from b4_16
where r307 != 2 and (r303 = 2 or r303 = 3)
```
### 18 - STATUS HUBUNGAN DENGAN KRT (R.303 kode 1, 2 3) DENGAN JENIS KELMAIN (R.304) SAMA 
```
%jdbc(hive)
use splf2020_16;

select pasangan_krt.id, pasangan_krt.kab_fullcode as kab, pasangan_krt.blok_sensus_fullcode as bs, pasangan_krt.nus as nus_pasangan, krt.nus as nus_krt, pasangan_krt.rosterIndex as no_urut_art_pasangan, pasangan_krt.r303 as hubungan_pasangan_krt, krt.rosterIndex as no_urut_art_krt, pasangan_krt.r304 as jk_pasangan, krt.r304 as jk_krt

from (
    select id, kab_fullcode, blok_sensus_fullcode, nus, rosterIndex, r303, r304, current_role
    from b4_16
    where r303 = 2 or r303 = 3
) pasangan_krt

left join (
    select id, kab_fullcode, blok_sensus_fullcode, nus, rosterIndex, r303, r304, current_role
    from b4_16
    where r303 = 1
) krt

on pasangan_krt.id = krt.id

where pasangan_krt.r304 = krt.r304
```
### 19 - WANITA PERNAH MELAHIRKAN (R.437 =1) DENGAN STATUS PERKAWINAN (R.307 KODE 1) 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, rosterIndex as no_urut_art, r437 as pernah_melahirkan, r307 as status_perkawinan, current_role as status_assignment
from b4_16 
where r437 = 1 and r307 = 1
```
### 20 - ART BERUMUR < 6 TAHUN MEMILIKI IJAZAH SD KE ATAS 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur, r431 as ijazah_tertinggi,
    CASE WHEN r431 = 1 THEN 'Belum/Tidak Pernah Sekolah'
    WHEN r431 = 2 THEN 'Belum/Tidak Tamat SD/SDLB/MI/Paket A'
    WHEN r431 = 3 THEN 'SD/SDLB/MI/Paket A'
    WHEN r431 = 4 THEN 'SMP/SMPLB/MTs/Paket B'
    WHEN r431 = 5 THEN 'SMA/SMLB/MA/SMK/MAK/Paket C'
    WHEN r431 = 6 THEN 'DI/DII/DIII'
    WHEN r431 = 7 THEN 'DIV/S1'
    WHEN r431 = 8 THEN 'Profesi'
    WHEN r431 = 9 THEN 'S2/S3'
    ELSE r431 END AS ijazah_tertinggi_ket,
    current_role as status_assignment
from b4_16 
where r306 < 6 and r431 > 2
```
### 21 - R.433 KODE 1 DENGAN STATUS DALAM PEKERJAAN R.436 KODE 5 ATAU 6 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r433, r436, current_role as status_assignment
from b4_16 
where (r433 = 1) and (r436 = 5 or r436 = 6)
```

### 22 - KONSISTENSI PERNAH MELAHIRKAN SEJAK 2021 DENGAN PERNAH MELAHIRKAN SEJAK 2017 (R.444=1 DAN R.442=2) 
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_art, r442 as melahirkan_hidup_2017, r444 as melahirkan_hidup_2021
FROM
    b4_16
WHERE 
    r442 = 2 and r444 = 1
```

### 23 - ART UMUR 5 TAHUN KE ATAS YANG BELUM/TIDAK PUNYA KK/KTP (R.402B=2) 
```
%jdbc(hive)

USE splf2020_16;

select 
    id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r306 AS umur, r301 as no_urut_art, r402b as alasan_nik_kosong
from
    b4_16
where 
    r402b = 2 and r306 >= 5
```
### 24 - KBLI & KBJI
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_art, r434, r434_kode AS kode_kbli, r435, r435_kode as kode_kbji
FROM
    b4_16
WHERE (r434_kode is not null) or (r435_kode is not null)
```
### 25 - R.606A ATAU R.606B TERISI (CEK UNTUK IMR) 
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, kab_fullcode as kab, blok_sensus_fullcode AS bs, nus, r606a as umur_meninggal_hari, r606b as umur_meninggal_bln, r606c as umur_meninggal_thn
FROM
    b6_16
WHERE (r606b is not null) or (r606a is not null)
```
### 26 - JUMLAH R.443A + R443B, DENGAN R.604, R.606A, R.606B TERISI 
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, kab_fullcode as kab, blok_sensus_fullcode AS bs, nus, r604 as jk_meninggal, r606a as umur_meninggal_hari, r606b as umur_meninggal_bln, r606c as umur_meninggal_thn
FROM
    b6_16
WHERE (r606b is not null) or (r606a is not null)
```
### 27 - ART BERUSIA 15++ R.432 = 1 ATAU R.433 = 1 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, rosterIndex as no_urut_art, r306 as umur, r432, r433
from b4_16
where (r306>=15) and ((r432 = 1) or (r433 = 1))
```

### 28 - ART BEKERJA DI SEKTOR PERTANIAN R.434 = 01 
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, rosterIndex as no_urut_art, r432, r433, r434, r434_kode
from b4_16
where (r434_kode = 1)
```

