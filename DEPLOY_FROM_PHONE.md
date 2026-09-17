# Deploy dari HP

1. GitHub > New repository > nama: `CryptoBBSakti-Whale-Radar` > Private.
2. Upload semua file dari ZIP ini ke root repository.
3. Railway > New Project > Deploy from GitHub repo > pilih repository tersebut.
4. Railway > service > Variables, tambahkan:
   - BIRDEYE_API_KEY
   - TELEGRAM_BOT_TOKEN
   - TELEGRAM_CHAT_ID
   - BIRDEYE_WS_URL (gunakan endpoint yang ditampilkan di dokumentasi/dashboard Birdeye plan Anda)
5. Tambahkan variabel filter lain dari `.env.example` bila ingin mengubah default.
6. Deploy.
7. Buka Logs. Cari `Birdeye WebSocket connected`.
8. Telegram harus menerima `CryptoBBSakti Whale Radar ONLINE`.
9. Jika WebSocket menolak endpoint/payload, jangan ubah file lain. Cocokkan `BIRDEYE_WS_URL` dan payload di `birdeye.py` dengan contoh WebSocket persis dari dashboard/docs akun Birdeye Anda.

PENTING: jangan pernah upload `.env` berisi secret ke GitHub.
