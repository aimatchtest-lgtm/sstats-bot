import os
import requests
from supabase import create_client
from datetime import datetime

# 1. Настройки из GitHub Secrets (они будут спрятаны)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

# Подключаемся к базе
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Настраиваем запросы к API sstats
headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

def main():
    print("🚀 Запуск сборщика данных...")
    
    try:
        # Тестовый запрос: берем список лиг, чтобы проверить связь
        resp = requests.get(f"{BASE}/leagues", headers=headers)
        data = resp.json()
        
        if data.get("status") == "OK":
            leagues = data.get("data", [])
            print(f"✅ Успешно получено {len(leagues)} лиг.")
            
            # Для теста возьмем первую лигу и сохраним факт успеха в базу
            if leagues:
                test_league = leagues[0]
                
                row = {
                    "external_id": f"test_{test_league.get('id')}",
                    "league_name": "TEST_CONNECTION",
                    "home_team": "System",
                    "away_team": "Check",
                    "match_time": datetime.now().isoformat(),
                    "status": "finished",
                    "score_home": 1,
                    "score_away": 0,
                    "updated_at": datetime.now().isoformat()
                }
                
                # Пишем в таблицу matches
                supabase.table("matches").insert(row).execute()
                print("💾 Данные успешно записаны в Supabase!")
            else:
                print("⚠️ Список лиг пуст.")
        else:
            print(f"❌ Ошибка API: {data}")
            
    except Exception as e:
        print(f"💥 Критическая ошибка: {str(e)}")

if __name__ == "__main__":
    main()
