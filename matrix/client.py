# Importujemy wymagane klasy i funkcje
from multiprocessing.managers import BaseManager
import sys
import time

# Tworzymy klasę QueueManager dziedziczącą po BaseManager
class QueueManager(BaseManager):
    pass

# Rejestrujemy kolejki, które będą wykorzystywane do wymiany danych z serwerem
QueueManager.register('get_queue_data')
QueueManager.register('get_queue_results')

def read(fname):
    """
    Funkcja wczytująca macierz lub wektor z pliku.
    """
    try:
        with open(fname, "r") as f:
            nr = int(f.readline().rstrip())  # Liczba wierszy
            nc = int(f.readline().rstrip())  # Liczba kolumn
            A = [[0] * nc for _ in range(nr)]  # Inicjalizujemy macierz A
            r = 0  # Indeks wiersza
            c = 0  # Indeks kolumny
            # Wczytujemy dane do macierzy
            for i in range(0, nr * nc):
                A[r][c] = float(f.readline().rstrip())  # Wczytujemy pojedynczy element
                c += 1
                if c == nc:  # Po wczytaniu całego wiersza
                    c = 0
                    r += 1
        return A
    except FileNotFoundError:
        print(f"Błąd: Plik '{fname}' nie został znaleziony.")
        sys.exit(1)
    except ValueError as ve:
        print(f"Błąd: Nieprawidłowy format danych w pliku '{fname}'. {ve}")
        sys.exit(1)
    except Exception as e:
        print(f"Błąd przy odczycie pliku '{fname}': {e}")
        sys.exit(1)

def split_indices(total, parts):
    """
    Funkcja dzieląca dane na części (indeksy).
    """
    base = total // parts  # Podstawowy rozmiar
    remainder = total % parts  # Reszta do podziału
    indices = []  # Lista indeksów
    start = 0  # Początkowy indeks

    # Dzielimy dane na części
    for i in range(parts):
        extra = 1 if i < remainder else 0  # Dodatkowy element w przypadku reszty
        end = start + base + extra  # Końcowy indeks
        indices.append((start, end))  # Dodajemy zakres
        start = end  # Przesuwamy początkowy indeks na koniec poprzedniego zakresu
    return indices

def main():
    start_time = time.time()

    # Sprawdzamy argumenty wejściowe
    if len(sys.argv) < 4:
        print("Użycie: python client.py <liczba_zadań> <plik_macierzy> <plik_wektora>")
        sys.exit(1)

    try:
        num_tasks = int(sys.argv[1])
        if num_tasks < 1:
            print("Liczba tasków nie może być < 1")
            sys.exit(1)
        fnameA = sys.argv[2]
        fnameX = sys.argv[3]
    except ValueError:
        print("Błąd: <liczba_zadań> musi być liczbą całkowitą.")
        sys.exit(1)
    except Exception as e:
        print(f"Błąd podczas przetwarzania argumentów: {e}")
        sys.exit(1)

    # Wczytujemy dane z plików
    A = read(fnameA)
    X = read(fnameX)

    # Spłaszczamy wektor X (przekształcamy z listy list na listę prostą)
    X = [row[0] for row in X]

    m = len(A)  # Liczba wierszy macierzy
    n = len(A[0])  # Liczba kolumn macierzy

    # Łączymy się z managerem
    try:
        mng = QueueManager(address=('192.168.0.45', 50000), authkey=b'abracadabra')
        mng.connect()
    except Exception as e:
        print(f"Błąd przy łączeniu z managerem: {e}")
        sys.exit(1)

    try:
        dataQueue = mng.get_queue_data()  # Pobieramy kolejkę danych
        resultsQueue = mng.get_queue_results()  # Pobieramy kolejkę wyników
    except Exception as e:
        print(f"Błąd przy pobieraniu kolejek: {e}")
        sys.exit(1)

    # Dzielimy zadania na części
    partitions = split_indices(m, num_tasks)

    for start, end in partitions:
        A_block = A[start:end]  # Dzielimy macierz na fragmenty
        dataQueue.put((start, A_block, X))  # Wysyłamy fragment do kolejki

    for _ in range(num_tasks):
        dataQueue.put(None) 
    
    
    final_result = [0] * m  # Lista na wynik końcowy

    print("Client: Czekam na wyniki...")
    received_tasks = 0
    while received_tasks < num_tasks:
        start_index, block_result = resultsQueue.get()  # Pobieramy wynik z kolejki
        block_length = len(block_result)  # Długość wynikowego fragmentu
        final_result[start_index:start_index + block_length] = block_result  # Łączymy wyniki
        received_tasks += 1

    # Wyświetlamy wynik końcowy
    print("Client: Wynik końcowy (iloczyn macierzy i wektora):")
    print(final_result)

    end_time = time.time()  # Zapisujemy czas końcowy
    elapsed_time = end_time - start_time  # Obliczamy czas wykonania
    print(f"Client: Czas wykonania: {elapsed_time:.4f} sekund")

if __name__ == '__main__':
    main()  # Uruchamiamy funkcję main()
