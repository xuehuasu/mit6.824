#include <iostream>
#include <vector>
#include <variant>
#include <cstring>
using namespace std;

class A {
public:
    A(){}
    // 拷贝构造
    A(const A& a) {
        cout << "拷贝构造" << endl;
    }
    A& operator=(const A& a) {
        cout << "拷贝赋值" << endl;
        return *this;
    }
    A func() {
        cout << "func" << endl;
        return *this;
    }
    // 重载<<
    friend ostream& operator<<(ostream& os, const A& a) {
        os << "重载<<" << endl;
        return os;
    }
};
class B
{
private:
    /* data */
public:
    B(/* args */);
    ~B();
    void func() {
        A a;
        cout << a.func();
    }
};

B::B(/* args */)
{
}

B::~B()
{
}

int main() {
    B b;
    b.func();
    return 0;
}