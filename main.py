import os
import requests
from supabase import create_client
from datetime import datetime, timezone
import time

# 1. Настройки
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

def safe_get(url):
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.json().get("data")
        elif r.status_code == 429:
            print("⚠️ Лимит API! Ждем 65 сек...")
            time.sleep(65)
            return safe_get(url)
        else:
            print(f"❌ Ошибка {r.status_code}: {url}")
            return None
    except Exception as e:
        print(f"💥 Сбой соединения: {e}")
        return None

def main():
    print("🚀 Запуск полного сборщика (Матчи + Статистика + Коэффициенты)...")
    
    # НАСТРОЙКА: Какую лигу собираем? 39 = АПЛ
    LEAGUE_ID = 39
    YEAR = 2024 
    
    print(f"Сбор матчей для Лиги ID: {LEAGUE_ID}, Год: {YEAR}")
    
    # 1. Получаем список матчей (лимит 50 для теста, чтобы не превысить лимиты API)
    games_list = safe_get(f"{BASE}/games/list?leagueid={LEAGUE_ID}&year={YEAR}&limit=50")
    
    if not games_list:
        print("❌ Не удалось получить список матчей.")
        return

    print(f"✅ Найдено {len(games_list)} матчей. Начинаем детальный сбор...")
    
    saved_matches = 0
    saved_odds = 0

    for game in games_list:
        gid = game.get("id")
        home_team = game.get("homeTeam", {}).get("name")
        away_team = game.get("awayTeam", {}).get("name")
        
        print(f"Обработка: {home_team} vs {away_team} (ID: {gid})")
        
        # 2. Получаем ДЕТАЛЬНЫЕ данные матча (статистика, судья, события)
        details = safe_get(f"{BASE}/games/{gid}")
        
        if not details:
            continue
            
        game_data = details.get("game", {})
        stats = details.get("statistics", {})
        referee = details.get("refereeName")
        
        # Парсинг времени
        date_str = game_data.get("date")
        match_time = None
        if date_str:
            try:
                clean_date = date_str.replace('Z', '+00:00')
                if '+' not in clean_date and '-' not in clean_date[10:]:
                     clean_date += '+00:00'
                match_time = datetime.fromisoformat(clean_date)
            except:
                pass

        # Подготовка строки для таблицы matches
        row_match = {
            "external_id": str(gid),
            "league_name": "Premier League",
            "home_team": home_team,
            "away_team": away_team,
            "match_time": match_time.isoformat() if match_time else None,
            "status": "finished" if game_data.get("status") in [8, 9, 10] else "scheduled",
            "score_home": game_data.get("homeFTResult"),
            "score_away": game_data.get("awayFTResult"),
            "ht_score_home": game_data.get("homeHTResult"),
            "ht_score_away": game_data.get("awayHTResult"),
            # Статистика (извлекаем из объекта statistics)
            "stats_yellow_cards_home": stats.get("yellowCardsHome") if stats else None,
            "stats_yellow_cards_away": stats.get("yellowCardsAway") if stats else None,
            "stats_corners_home": stats.get("cornerKicksHome") if stats else None,
            "stats_corners_away": stats.get("cornerKicksAway") if stats else None,
            "stats_fouls_home": stats.get("foulsHome") if stats else None,
            "stats_xg_home": stats.get("expectedGoalsHome") if stats else None,
            "stats_xg_away": stats.get("expectedGoalsAway") if stats else None,
            "referee_name": referee,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Сохраняем или обновляем матч
            existing = supabase.table("matches").select("id").eq("external_id", str(gid)).execute()
            if existing.data:
                supabase.table("matches").update(row_match).eq("external_id", str(gid)).execute()
            else:
                supabase.table("matches").insert(row_match).execute()
            saved_matches += 1
            
            # 3. Сбор КОЭФФИЦИЕНТОВ (если есть)
            odds_response = safe_get(f"{BASE}/odds/{gid}")
            if odds_response:
                for bookmaker_odds in odds_response:
                    bookmaker_name = bookmaker_odds.get("bookmakerName")
                    bets = bookmaker_odds.get("odds", [])
                    
                    for bet in bets:
                        market_name = bet.get("marketName") # например "1X2"
                        prices = bet.get("odds", []) # список котировок
                        
                        for price in prices:
                            selection = price.get("name") # Home, Draw, Away
                            value = price.get("value")
                            opening_value = price.get("openingValue")
                            
                            if not all([market_name, bookmaker_name, selection, value]):
                                continue
                                
                            # Сохраняем закрывающий коэффициент
                            row_odds_close = {
                                "match_external_id": str(gid),
                                "market_type": market_name,
                                "bookmaker": bookmaker_name,
                                "selection": selection,
                                "odd_value": float(value),
                                "is_opening": False,
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }
                            supabase.table("odds_snapshots").insert(row_odds_close).execute()
                            saved_odds += 1

                            # Если есть открывающий коэффициент - сохраняем его тоже
                            if opening_value:
                                row_odds_open = {
                                    "match_external_id": str(gid),
                                    "market_type": market_name,
                                    "bookmaker": bookmaker_name,
                                    "selection": selection,
                                    "odd_value": float(opening_value),
                                    "is_opening": True,
                                    "timestamp": datetime.now(timezone.utc).isoformat()
                                }
                                supabase.table("odds_snapshots").insert(row_odds_open).execute()
                                saved_odds += 1
                            
        except Exception as e:
            print(f"Ошибка сохранения: {e}")
            
        # Пауза, чтобы не забанили за спам запросами (важно для бесплатного тарифа)
        time.sleep(1.5) 

    print(f"💾 Готово! Матчей: {saved_matches}, Коэффициентов: {saved_odds}")

if __name__ == "__main__":
    main()
