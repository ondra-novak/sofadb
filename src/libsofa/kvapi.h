/*
 * kvapi.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KVAPI_H_
#define SRC_LIBSOFA_KVAPI_H_

#include <shared/refcnt.h>
#include <utility>
#include <string_view>

namespace sofadb {

	using ondra_shared::RefCntObj;
	using ondra_shared::RefCntPtr;

	using KeyValue = std::pair<std::string_view,std::string_view>;

	///Only allows to iterate one direction of a range.
	class AbstractIterator: public RefCntObj {
	public:

		virtual bool getNext(KeyValue &row) = 0;

		virtual ~AbstractIterator() {}


	};

	using PIterator = RefCntPtr<AbstractIterator>;

	class Iterator: public PIterator {
	public:
		using PIterator::PIterator;

		bool getNext(KeyValue &row) {return this->ptr->getNext(row);}

		bool getNext() {
			return getNext(tmp);
		}

		const KeyValue *operator->() const {return &tmp;}
		const KeyValue &operator *() const {return tmp;}

	protected:
		KeyValue tmp;

	};

	class AbstractChangeset: public RefCntObj {
	public:

		virtual void put(const std::string_view &key, const std::string_view &value) = 0;

		virtual void erase(const std::string_view &key) = 0;

		virtual void commit() = 0;

		virtual void rollback() = 0;

		virtual ~AbstractChangeset() {}

	};

	using PChangeset = RefCntPtr<AbstractChangeset>;


	class AbstractKeyValueDatabase:public RefCntObj{
	public:

		virtual PChangeset createChangeset();

		virtual PIterator find_range(const std::string_view &prefix, bool reverse = false) = 0;

		virtual PIterator find_range(const std::string_view &start, std::string_view &end, bool reverse = false) = 0;

		virtual PIterator find(const std::string_view &prefix) = 0;

		virtual bool lookup(const std::string_view &key, std::string &value)  = 0;

		virtual std::size_t getIndexSize(const std::string_view &prefix) const = 0;

		virtual std::size_t getIndexSize(const std::string_view &start, std::string_view &end) const = 0;

		virtual ~AbstractKeyValueDatabase() {};


	};

	using PKeyValueDatabase = RefCntPtr<AbstractKeyValueDatabase>;

	class AbstractKeyValueFactory: public RefCntObj {
	public:

		virtual PKeyValueDatabase create(std::string_view name) = 0;

		virtual ~AbstractKeyValueFactory() {};
	};



}



#endif /* SRC_LIBSOFA_KVAPI_H_ */
