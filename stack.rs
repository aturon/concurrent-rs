// Treiber's stack with hazard pointers

// FIXME: shouldn't core::ptr::RawPtr::to_option be as_option?

// FIXME: audit memory orderings throughout

// FIXME: add a `live` or `dead` count for hazard pointers to avoid traversal on
// clone and drop (akin to refcounting)

#![allow(dead_code)]

use std::sync::atomics::{AtomicBool, AtomicPtr, SeqCst, Relaxed};
use std::ptr;
use std::mem;
use std::sync::RWLock;

// Used for padding to avoid false sharing.
static CACHE_LINE_SIZE: uint = 64;

// The shared Stack data structure, which is not exported or used directly by
// clients; all access must go through a StackHandle<T>, which is akin to
// Arc<Stack<T>> but incorporates hazard pointers.
struct Stack<T> {
    handle_data: RWLock<Vec<*const Hazard<T>>>,
    head: AtomicPtr<Node<T>>,
}

struct Hazard<T> {
    alive: AtomicBool,             // is the handle that created this hazard pointer sill alive?
    ptr: AtomicPtr<Node<T>>,       // a root pointer for GC purposes
    pad: [u8, ..CACHE_LINE_SIZE],  // pad to cache line size to avoid false sharing
}

impl<T> Hazard<T> {
    fn new() -> Hazard<T> {
        Hazard {
            alive: AtomicBool::new(true),
            ptr: AtomicPtr::new(ptr::mut_null()),
            pad: [0, ..CACHE_LINE_SIZE],
        }
    }
}

struct Node<T> {
    data: T,
    tail: *mut Node<T>,
}

pub struct StackHandle<T> {
    stack: *const Stack<T>,
    hazard: *const Hazard<T>,
    to_free: Vec<*mut Node<T>>,
}

// for ease of type inference
unsafe fn into_ptr<T>(t: T) -> *const T {
    mem::transmute(box t)
}

impl<T: Send> StackHandle<T> {
    pub fn new() -> StackHandle<T> {
        unsafe {
            let hazard: *const Hazard<T> = into_ptr(Hazard::new());
            let stack: *const Stack<T> = into_ptr(Stack {
                handle_data: RWLock::new(vec!(hazard)),
                head: AtomicPtr::new(ptr::mut_null()),
            });
            StackHandle {
                stack: stack,
                hazard: hazard,
                to_free: Vec::new(),
            }
        }
    }

    pub fn push(&mut self, val: T) {
        unsafe {
            let n: *mut Node<T> = mem::transmute(box Node {
                data: val,
                tail: ptr::mut_null(),
            });

            loop {
                let snapshot = (*self.stack).head.load(Relaxed);
                (*n).tail = snapshot;
                if (*self.stack).head.compare_and_swap(snapshot, n, SeqCst) == snapshot {
                    return
                }
            }
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        unsafe {
            loop {
                let snapshot = (*self.stack).head.load(Relaxed);
                if snapshot.is_null() { return None };
                (*self.hazard).ptr.store(snapshot, SeqCst);  // the SeqCst here may not be necssary
                if (*self.stack).head.compare_and_swap(snapshot, (*snapshot).tail, SeqCst) == snapshot {
                    let data = ptr::read(&(*snapshot).data);
                    self.to_free.push(snapshot);
                    return Some(data);
                }
                (*self.hazard).ptr.store(ptr::mut_null(), Relaxed);
            }
        }
    }

    // garbage collect the nodes delinked via this handle
    pub fn gc(&mut self) {
        unsafe {
            if self.to_free.is_empty() { return }

            let snapshot = {
                let handle_data = (*self.stack).handle_data.read();
                handle_data.iter().filter_map(|h| {
                    if (**h).alive.load(Relaxed) {
                        Some((**h).ptr.load(SeqCst))
                    } else {
                        None
                    }
                }).collect::<Vec<*mut Node<T>>>()
            };

            // FIXME: is there a better way to do this dance?
            let mut to_free = Vec::new();
            mem::swap(&mut self.to_free, &mut to_free);
            for h in to_free.move_iter() {
                if snapshot.iter().any(|other| *other == h) {
                    self.to_free.push(h)
                } else {
                    let n: Box<Node<T>> = mem::transmute(h);
                    drop(n)
                }
            }
        }
    }
}

impl<T> Clone for StackHandle<T> {
    fn clone(&self) -> StackHandle<T> {
        unsafe fn new_hazard<T>(handle_data: &RWLock<Vec<*const Hazard<T>>>) -> *const Hazard<T> {
            // take the writer lock
            let mut handle_data = handle_data.write();

            // look for reusable hazard pointers
            for h in handle_data.iter().map(|h| *h) {
                if !(*h).alive.load(Relaxed) {
                    (*h).ptr.store(ptr::mut_null(), Relaxed);
                    (*h).alive.store(true, Relaxed);
                    return h;
                }
            }

            // no luck, make a new one
            let hazard = into_ptr(Hazard::new());
            handle_data.push(hazard);
            hazard
        }

        StackHandle {
            stack: self.stack,
            hazard: unsafe { new_hazard(&(*self.stack).handle_data) },
            to_free: Vec::new(),
        }
    }
}

#[unsafe_destructor]
impl<T: Send> Drop for StackHandle<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.hazard).alive.store(false, Relaxed);
            while !self.to_free.is_empty() { self.gc() }

            let mut handle_data = (*self.stack).handle_data.write();
            if handle_data.iter().all(|h| !(**h).alive.load(Relaxed)) {
                let stack: Box<Stack<T>> = mem::transmute(self.stack);
                drop(stack);
            }
        }
    }
}
