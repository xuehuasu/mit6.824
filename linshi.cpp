#include <iostream>
using namespace std;
const int N = 1e5+7;
int nums[N];
int size = 0;

void up(int x) {
    while (x > 0) {
        if (x != 1 && nums[x] > nums[x >> 1]) {
            swap(nums[x], nums[x >> 1]);
            x >>= 1;
        }
        else break;
    }
}

void down(int x) {
    while (x <= size && (x << 1) <= size) {
        int  t = x << 1;
        if (t + 1 <= size && nums[t] < nums[t + 1]) t ++;
        if (nums[x] > nums[t]) break;
        swap(nums[x], nums[t]);
        x = t;
    }
}

void add(int x) {
    nums[++ size] = x;
    up(x);
}

int pop() {
    int t = nums[1];
    nums[1] = nums[size];
    size --;
    down(1);
    return t;
}

int main() {
    int n, k;
    cin >> n >> k;
    int x;
    for (int i = 0; i < n; i ++) {
        cin >> x;
        add(x);
    }
    for (int i = 0; i < n; i ++) {
        cout << pop() << endl;
    }
}
// 64 位输出请用 printf(\"%lld\")