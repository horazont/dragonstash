/**********************************************************************
File name: main.cpp
This file is part of: DragonStash

LICENSE

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.

FEEDBACK & QUESTIONS

For feedback and questions about DragonStash please e-mail one of the
authors named in the AUTHORS file.
**********************************************************************/
#include <lmdb-safe.hh>

void clear(MDBEnv &env, MDBDbi &dbi) {
    auto trans = env.getRWTransaction();
    auto cursor = trans->getCursor(dbi);
    MDBOutVal key{};
    MDBOutVal value{};
    int rc;
    while ((rc = cursor.get(key, value, MDB_FIRST)) == 0) {
        std::cout << key.get<std::uint64_t>() << " " << cursor.del() << std::endl;
    }

    if (rc != MDB_NOTFOUND) {
        throw std::runtime_error("cursor.get(..., MDB_FIRST) returned "+std::to_string(rc));
    }
    trans->commit();
}

static constexpr std::uint64_t nkeys = 3;
static constexpr std::uint64_t keystep = 2;
static constexpr std::uint64_t nvalues = 10;
static constexpr std::uint64_t shift_key_into_value = 32;

void genkeys(MDBEnv &env, MDBDbi &dbi) {
    auto trans = env.getRWTransaction();
    for (std::uint64_t i = 0; i < nkeys * keystep; i += keystep) {
        for (std::uint64_t j = 0; j < nvalues; ++j) {
            MDBInVal key(i);
            MDBInVal value(j | (i << shift_key_into_value));
            trans->put(dbi, key, value);
        }
    }
    trans->commit();
}

void printpairs_plain(MDBEnv &env, MDBDbi &dbi) {
    auto trans = env.getROTransaction();
    auto cursor = trans->getCursor(dbi);
    int rc;
    MDBOutVal key{};
    MDBOutVal value{};
    MDB_cursor_op op = MDB_FIRST;
    while ((rc = cursor.nextprev(key, value, op)) == 0) {
        std::cout << std::hex <<
                     "k = " << key.get<std::uint64_t>() <<
                     "; v = " << value.get<std::uint64_t>() << std::endl;
        op = MDB_NEXT;
    }
}

void printpairs_dup(MDBEnv &env, MDBDbi &dbi) {
    auto trans = env.getROTransaction();
    auto cursor = trans->getCursor(dbi);
    int rc;
    MDBOutVal key{};
    MDBOutVal value{};
    MDB_cursor_op op = MDB_FIRST;
    while ((rc = cursor.nextprev(key, value, op)) == 0) {
        std::cout << std::hex <<
                     "k = " << key.get<std::uint64_t>() <<
                     "; v = " << value.get<std::uint64_t>() << std::endl;
        op = MDB_NEXT_DUP;
    }
}

void printpairs_nodup(MDBEnv &env, MDBDbi &dbi) {
    auto trans = env.getROTransaction();
    auto cursor = trans->getCursor(dbi);
    int rc;
    MDBOutVal key{};
    MDBOutVal value{};
    MDB_cursor_op op = MDB_FIRST;
    while ((rc = cursor.nextprev(key, value, op)) == 0) {
        std::cout << std::hex <<
                     "k = " << key.get<std::uint64_t>() <<
                     "; v = " << value.get<std::uint64_t>() << std::endl;
        op = MDB_NEXT_NODUP;
    }
}

void printpairs_search(MDBEnv &env, MDBDbi &dbi, std::uint64_t key,
                       MDB_cursor_op initop = MDB_SET) {
    auto trans = env.getROTransaction();
    auto cursor = trans->getCursor(dbi);
    int rc;
    MDBInVal key_in(key);
    MDBOutVal keyv{};
    MDBOutVal value{};
    std::cout << "printing entries for key " << key << std::endl;
    if (initop == MDB_SET) {
        rc = cursor.find(key_in, keyv, value);
    } else {
        rc = cursor.lower_bound(key_in, keyv, value);
    }
    if (rc == MDB_NOTFOUND) {
        return;
    }
    do {
        std::cout << std::hex <<
                     "k = " << keyv.get<std::uint64_t>() <<
                     "; v = " << value.get<std::uint64_t>() << std::endl;
    } while ((rc = cursor.nextprev(keyv, value, MDB_NEXT_DUP)) == 0);
}

/* int main(int argc, char **argv) {
    (void)argc;
    (void)argv;
    auto env = getMDBEnv("./test", MDB_NOSUBDIR, 0644);
    auto db = env->openDB("dirs", MDB_DUPSORT | MDB_CREATE);
    clear(*env, db);
    genkeys(*env, db);
    printpairs_plain(*env, db);
    printpairs_dup(*env, db);
    printpairs_nodup(*env, db);

    printpairs_search(*env, db, 2);
    printpairs_search(*env, db, 1);
    printpairs_search(*env, db, 3);

    printpairs_search(*env, db, 1, MDB_SET_RANGE);
} */

int main(int argc, char **argv) {

}
