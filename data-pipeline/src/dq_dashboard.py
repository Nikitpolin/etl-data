import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_CONFIG
except ImportError:
    # Альтернативный способ если импорт не работает
    DB_CONFIG = {
        'host': 'localhost',
        'port': '5432',
        'database': 'etl_db',
        'user': 'user',
        'password': 'password'
    }

def generate_dq_dashboard(days_back=7):
    """
    Генерирует простой текстовый дашборд качества данных
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        summary_query = """
            SELECT 
                COUNT(*) as total_checks,
                COUNT(*) FILTER (WHERE status = 'passed') as passed_checks,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_checks,
                COUNT(*) FILTER (WHERE status = 'error') as error_checks,
                ROUND(COUNT(*) FILTER (WHERE status = 'passed') * 100.0 / NULLIF(COUNT(*), 0), 2) as success_rate
            FROM s_sql_dds.t_dq_check_results
            WHERE execution_date >= CURRENT_DATE - INTERVAL '%s days'
        """
        
        cur = conn.cursor()
        cur.execute(summary_query, (days_back,))
        result = cur.fetchone()
        
        if result and result[0] > 0:
            total, passed, failed, errors, success_rate = result
            print(f" Период: последние {days_back} дней")
            print(f" Всего проверок: {total}")
            print(f" Успешных: {passed} ({success_rate}%)")
            print(f" Неудачных: {failed}")
            print(f"!  Ошибок: {errors}")
            
            # Визуализация прогресс-бара
            bar_length = 40
            passed_length = int(passed / total * bar_length) if total > 0 else 0
            failed_length = int(failed / total * bar_length) if total > 0 else 0
            error_length = bar_length - passed_length - failed_length
            
            print(f"\n Прогресс: |{'|' * passed_length}{'|' * failed_length}{'|' * error_length}|")
            print(f"            |{passed_length*' '}P{failed_length*' '}F{error_length*' '}E|")
            print("            P=Passed, F=Failed, E=Error")
        else:
            print("Нет данных о проверках за указанный период")
        
        
        type_query = """
            SELECT 
                check_type,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'passed') as passed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'error') as errors,
                ROUND(COUNT(*) FILTER (WHERE status = 'passed') * 100.0 / NULLIF(COUNT(*), 0), 2) as success_rate
            FROM s_sql_dds.t_dq_check_results
            WHERE execution_date >= CURRENT_DATE - INTERVAL '%s days'
              AND check_type != 'summary'
            GROUP BY check_type
            ORDER BY check_type
        """
        
        cur.execute(type_query, (days_back,))
        type_results = cur.fetchall()
        
        if type_results:
            print(f"{'Тип проверки':<20} {'Всего':<6} {'+':<6} {'-':<6} {'!':<6} {'%':<6}")
            print("-"*50)
            for row in type_results:
                check_type, total, passed, failed, errors, rate = row
                print(f"{check_type:<20} {total:<6} {passed:<6} {failed:<6} {errors:<6} {rate:<6.1f}")
        else:
            print("Нет данных о типах проверок")
        
        
        recent_query = """
            SELECT 
                check_name,
                status,
                execution_date,
                error_message
            FROM s_sql_dds.t_dq_check_results
            WHERE check_type != 'summary'
            ORDER BY execution_date DESC
            LIMIT 10
        """
        
        cur.execute(recent_query)
        recent_results = cur.fetchall()
        
        if recent_results:
            for check_name, status, exec_date, error_msg in recent_results:
                status_icon = "+" if status == 'passed' else "-" if status == 'failed' else "!"
                time_str = exec_date.strftime("%H:%M:%S") if exec_date else "N/A"
                print(f"{status_icon} [{time_str}] {check_name}")
                if error_msg and status != 'passed':
                    print(f"   ↳ {error_msg[:60]}...")
        else:
            print("Нет данных о последних проверках")
        
        
        trends_query = """
            SELECT 
                DATE(execution_date) as check_date,
                COUNT(*) as total_checks,
                COUNT(*) FILTER (WHERE status = 'passed') as passed_checks,
                ROUND(COUNT(*) FILTER (WHERE status = 'passed') * 100.0 / NULLIF(COUNT(*), 0), 2) as daily_success_rate
            FROM s_sql_dds.t_dq_check_results
            WHERE execution_date >= CURRENT_DATE - INTERVAL '%s days'
              AND check_type != 'summary'
            GROUP BY DATE(execution_date)
            ORDER BY check_date
        """
        
        cur.execute(trends_query, (days_back,))
        trends = cur.fetchall()
        
        if trends:
            print(f"{'Дата':<12} {'Проверок':<10} {'Успешно':<10} {'%':<8}")
            print("-"*40)
            for check_date, total, passed, rate in trends:
                date_str = check_date.strftime("%d.%m")
                trend_arrow = "↑" if rate >= 95 else "↓" if rate <= 80 else "→"
                print(f"{date_str:<12} {total:<10} {passed:<10} {rate:<7.1f} {trend_arrow}")
        else:
            print("Нет данных о трендах")
        
        
        critical_query = """
            SELECT 
                check_name,
                error_message,
                execution_date
            FROM s_sql_dds.t_dq_check_results
            WHERE status IN ('failed', 'error')
              AND execution_date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY execution_date DESC
            LIMIT 5
        """
        
        cur.execute(critical_query, (days_back,))
        critical_issues = cur.fetchall()
        
        if critical_issues:
            for i, (check_name, error_msg, exec_date) in enumerate(critical_issues, 1):
                date_str = exec_date.strftime("%d.%m %H:%M") if exec_date else "N/A"
                print(f"{i}. {check_name} ({date_str})")
                if error_msg:
                    print(f"   {error_msg[:80]}...")
        else:
            print("Критических проблем не обнаружено!")
        
        
        if 'success_rate' in locals() and success_rate:
            if success_rate < 90:
                print("Проверить качество входящих данных")
                print("Рассмотреть возможность корректировки порогов проверок")
            
            if failed > 0:
                print("Проанализировать неудачные проверки")
                print("Внедрить корректирующие действия для проблемных областей")
            
            if errors > 0:
                print("Проверить стабильность системы проверок")
                print("Убедиться в доступности всех зависимых систем")
            
            if success_rate >= 90 and failed == 0 and errors == 0:
                print("Продолжать текущие практики контроля качества")
                print("Рассмотреть возможность добавления новых проверок")
        
        
        if 'success_rate' in locals() and success_rate:
            if success_rate >= 95:
                print("ОТЛИЧНО: Качество данных на высоком уровне")
                print("Все системы работают стабильно")
            elif success_rate >= 85:
                print("УДОВЛЕТВОРИТЕЛЬНО: Незначительные проблемы")
                print("Требуется мониторинг и корректировка")
            else:
                print("ТРЕБУЕТ ВНИМАНИЯ: Значительные проблемы качества")
                print("Необходимы срочные корректирующие действия")
        else:
            print("НЕИЗВЕСТНО: Нет данных для оценки статуса системы")
        
        cur.close()
        print("\n" + "="*80)
        print(f"Дашборд сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"Ошибка при генерации дашборда: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Получаем количество дней из аргументов командной строки
    days_back = 7
    if len(sys.argv) > 1:
        try:
            days_back = int(sys.argv[1])
        except ValueError:
            print(f"Неверный аргумент: {sys.argv[1]}. Использую значение по умолчанию: 7 дней")
    
    generate_dq_dashboard(days_back)