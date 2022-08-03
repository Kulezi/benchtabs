#ifndef SEMAPHORE_HPP
#define SEMAPHORE_HPP

#include <condition_variable>
#include <mutex>

// Taken from https://vorbrodt.blog/2019/02/05/fast-semaphore/
class Semaphore {
  public:
    Semaphore(int64_t initial_permits) noexcept : free_permits(initial_permits) {}

    void acquire_permit() noexcept {
        std::unique_lock<std::mutex> lock(sem_mutex);
        sem_cv.wait(lock, [&]() { return free_permits != 0; });
        free_permits -= 1;
    }

    void free_permit() noexcept {
        {
            std::unique_lock<std::mutex> lock(sem_mutex);
            free_permits += 1;
        }

        sem_cv.notify_one();
    }

  private:
    int64_t free_permits;
    std::mutex sem_mutex;
    std::condition_variable sem_cv;
};

#endif