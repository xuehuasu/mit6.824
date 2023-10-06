#include <iostream>
#include <vector>
#include <variant>
#include <cstring>
using namespace std;

template <class Input>
class A {
public:
    virtual bool Foo(Input input) = 0;
};

class B : public A<int> {
public:
    bool Foo(int input) override {
        cout << "B::Foo" << endl;
        return true;
    }
};

class C : public A<double> {
public:
    bool Foo(double input) override {
        cout << "C::Foo" << endl;
        return true;
    }
};

int main() {
    vector<variant<B, C>> vec;

    B b;
    C c;

    vec.push_back(b);
    vec.push_back(c);

    for (const auto& element : vec) {
        visit([](const auto& obj) {
            obj.Foo(42);
        }, element);
    }

    return 0;
}