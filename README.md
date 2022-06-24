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

### 01 Pengecekan anak yang berumur <= 15 tahun (R.306) dengan status perkawinan (R.307 kode 2,3,atau 4)
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur, r307 as status_perkawinan
from b4_16 
where r306 <= 15 and (r307 = 2 or r307 = 3 or r307 = 4)
```

### 02 Pengecekan status hubungan dengan KRT (R.303 kode 5,7,8) dengan status perkawinan (R.307 kode 1)
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r303 as hubungan_dengan_krt, r307 status_perkawinan
from b4_16
where r307 = 1 and (r303 = 5 or r303 = 7 or r303 = 8)
```

### 03 Pengecekan status hubungan dengan KRT (R.303 kode 2 atau 3) dengan status perkawinan (R.307 kode selain 2)
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r303 as hubungan_dengan_krt, r307 status_perkawinan
from b4_16
where r307 != 2 and (r303 = 2 or r303 = 3)
```
### 04 Pengecekan status hubungan dengan KRT (R.303 kode 1 dengan 2 dan 3) dengan jenis kelamin (R.304) tidak boleh sama
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

### 05 Pengecekan umur (R.306) >95 tahun
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur, current_role as status_assignment
from b4_16 
where r306 > 95
```

### 06 Pengecekan wanita pernah melahirkan (R.437 =1) dengan status perkawinan (R.307 kode 1)
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, rosterIndex as no_urut_art, r437 as pernah_melahirkan, r307 as status_perkawinan, current_role as status_assignment
from b4_16 
where r437 = 1 and r307 = 1
```

### 07 Pengecekan isian umur yang masih kosong atau null
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur, current_role as status_assignment
from b4_16 
where isnull(r306)
```
### 08 Pengecekan ijazah tertinggi lebih tinggi dari tidak tamat SD sederajat (R.431>2)  untuk art yang umurnya null atau kurang dari 6 tahun
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
### 09 Pengecekan tahun 2022 dikurang Tahun Kelahiran (R.305) lebih dari umur (R.306)+1 -> (2022-R.305 > R.306+1)
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r305a as tgl_lahir, r305b as bln_lahir, r305c as thn_lahir, r306 as umur_fasih,  (2022 - r305c) as umur_mod, current_role as status_assignment
from b4_16 
where (2022 - r305c) > (r306 + 1) and (r306 != 95)
```
### 10 Pengecekan R.433 kode 1 dengan status dlm pekerjaan R.436 kode 5 atau 6
```
%jdbc(hive)
use splf2020_16;

select id, kab_fullcode as kab, blok_sensus_fullcode as bs, nus, r433, r436, current_role as status_assignment
from b4_16 
where (r433 = 1) and (r436 = 5 or r436 = 6)
```
### 11 Jumlah Anak Lahir Hidup (R 438) yang lebih kecil daripada jumlah Anak seluruhnya (R439 + R440 +R441)
```
%jdbc(hive)

USE SPLF2020_16;

SELECT 
    id, blok_sensus_fullcode AS BS, nus, r301 AS no_urut_art, r438 AS jumlah_anak_lahir_hidup, (r439a+r439b+r440a+r440b+r441a+r441b) AS jumlah_anak_seluruhnya
FROM
    b4_16
WHERE 
    r438 < (r439a+r439b+r440a+r440b+r441a+r441b)
    

```

### 12 Pengecekan pernah melahirkan sejak 2021 dengan pernah melahirkan sejak 2017 (R.444=1 dan R.442=2)
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