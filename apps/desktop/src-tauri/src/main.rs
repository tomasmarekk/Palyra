//! Binary entrypoint for the Palyra desktop control center.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    palyra_desktop_control_center::run();
}
