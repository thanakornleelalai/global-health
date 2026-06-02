# Global Health ELT Pipeline (GCS → BigQuery)

โปรเจกต์นี้เป็น **ELT Pipeline** บน [Apache Airflow](https://airflow.apache.org/) สำหรับประมวลผลข้อมูลสถิติสุขภาพระดับโลก (Global Health Statistics) โดยดึงไฟล์ CSV จาก Google Cloud Storage (GCS) เข้าสู่ Google BigQuery แล้วแปลงข้อมูลออกเป็นตารางและวิว (View) แยกตามรายประเทศ

---

## 🔄 ภาพรวมการทำงาน (Pipeline Flow)

```
GCS (CSV)  →  BigQuery [staging]  →  BigQuery [transformed]  →  BigQuery [reporting]
  ตรวจไฟล์      โหลดข้อมูลดิบ           สร้างตารางรายประเทศ          สร้างวิวสำหรับรายงาน
```

ลำดับการทำงานของ DAG (`elt_pipeline_global_health`):

1. **`check_if_file_exists`** — ตรวจสอบว่าไฟล์ `global_health_statistics.csv` มีอยู่ใน GCS bucket หรือยัง (รอสูงสุด 300 วินาที, เช็กทุก 30 วินาที)
2. **`load_csv_to_bigquery`** — โหลด CSV จาก GCS เข้าสู่ตาราง staging โดยใช้ `autodetect` schema และเขียนทับข้อมูลเดิม (`WRITE_TRUNCATE`)
3. **`create_table_<country>`** — สำหรับแต่ละประเทศ สร้างตารางใน dataset `transformed` โดยกรองเฉพาะข้อมูลของประเทศนั้น ๆ
4. **`create_view_<country>`** — สร้างวิวใน dataset `reporting` แสดงเฉพาะข้อมูลโรคที่ **ยังไม่มีวัคซีนหรือการรักษา** (`Availability_of_Vaccines_or_Treatment = FALSE`)
5. **`success`** — task สุดท้ายที่รวมทุก view เข้าด้วยกันเพื่อยืนยันว่า pipeline เสร็จสมบูรณ์

---

## 📁 โครงสร้างไฟล์

| ไฟล์ | คำอธิบาย |
|------|----------|
| `global_healthdata_gcs_to_bq.py` | นิยาม Airflow DAG หลักของ pipeline |
| `create_table.sql` | เทมเพลต SQL สร้างตารางรายประเทศใน dataset `transformed` |
| `create_view.sql` | เทมเพลต SQL สร้างวิวรายงานใน dataset `reporting` |

> หมายเหตุ: เมื่อ deploy บน Airflow ไฟล์ SQL ทั้งสองควรอยู่ในโฟลเดอร์ `sql_scripts/` ตามค่าตัวแปร `QUERY_SQL_PATH`

---

## ⚙️ การตั้งค่า (Configuration)

ค่าหลัก ๆ ถูกกำหนดไว้ในไฟล์ `global_healthdata_gcs_to_bq.py`:

| ตัวแปร | ค่า | คำอธิบาย |
|--------|-----|----------|
| `project_id` | `apt-summer-479708-b8` | Google Cloud Project ID |
| `bucket_name` | `apt-summer-479708-b8-landing` | GCS bucket ที่เก็บไฟล์ต้นทาง |
| `file_name` | `global_health_statistics.csv` | ชื่อไฟล์ CSV ต้นทาง |
| `staging_dataset_id` | `staging` | Dataset เก็บข้อมูลดิบ |
| `transformed_dataset_id` | `transformed` | Dataset เก็บตารางรายประเทศ |
| `reporting_dataset_id` | `reporting` | Dataset เก็บวิวสำหรับรายงาน |

**ประเทศที่ประมวลผล (20 ประเทศ):** Argentina, Australia, Brazil, Canada, China, France, Germany, India, Indonesia, Italy, Japan, Mexico, Nigeria, Russia, Saudi Arabia, South Africa, South Korea, Turkey, UK, USA

---

## 🚀 วิธีใช้งาน

1. อัปโหลดไฟล์ `global_health_statistics.csv` ขึ้น GCS bucket ที่กำหนด
2. วางไฟล์ DAG (`global_healthdata_gcs_to_bq.py`) ไว้ในโฟลเดอร์ `dags/` ของ Airflow และวางไฟล์ SQL ไว้ใน `dags/sql_scripts/`
3. ตั้งค่า Google Cloud Connection ใน Airflow ให้มีสิทธิ์เข้าถึง GCS และ BigQuery
4. เปิดใช้งานและ trigger DAG `elt_pipeline_global_health` ผ่าน Airflow UI (ค่า `schedule_interval=None` หมายความว่ารันแบบ manual)

---

## 🛠️ เทคโนโลยีที่ใช้

- **Apache Airflow** — orchestration ของ pipeline
- **Google Cloud Storage (GCS)** — เก็บไฟล์ข้อมูลต้นทาง
- **Google BigQuery** — Data Warehouse สำหรับเก็บและแปลงข้อมูล
