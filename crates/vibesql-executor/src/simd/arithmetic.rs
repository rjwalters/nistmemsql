//! SIMD arithmetic operations for columnar data

#[cfg(feature = "simd")]
use wide::*;

/// SIMD addition for f64 columns
#[cfg(feature = "simd")]
pub fn simd_add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD (f64x4)
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let sum = a_vec + b_vec;

        let arr: [f64; 4] = sum.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// SIMD subtraction for f64 columns
#[cfg(feature = "simd")]
pub fn simd_sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let diff = a_vec - b_vec;

        let arr: [f64; 4] = diff.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// SIMD multiplication for f64 columns
#[cfg(feature = "simd")]
pub fn simd_mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let product = a_vec * b_vec;

        let arr: [f64; 4] = product.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}

/// SIMD division for f64 columns
#[cfg(feature = "simd")]
pub fn simd_div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let quotient = a_vec / b_vec;

        let arr: [f64; 4] = quotient.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] / b[i]);
    }

    result
}

/// SIMD addition for i64 columns
#[cfg(feature = "simd")]
pub fn simd_add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let sum = a_vec + b_vec;

        let arr: [i64; 4] = sum.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// SIMD subtraction for i64 columns
#[cfg(feature = "simd")]
pub fn simd_sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let diff = a_vec - b_vec;

        let arr: [i64; 4] = diff.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// SIMD multiplication for i64 columns
#[cfg(feature = "simd")]
pub fn simd_mul_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let product = a_vec * b_vec;

        let arr: [i64; 4] = product.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}

/// SIMD division for i64 columns
#[cfg(feature = "simd")]
pub fn simd_div_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let quotient = a_vec / b_vec;

        let arr: [i64; 4] = quotient.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] / b[i]);
    }

    result
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

    #[test]
    fn test_simd_add_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let result = simd_add_f64(&a, &b);
        assert_eq!(result, vec![11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0, 99.0]);
    }

    #[test]
    fn test_simd_sub_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let b = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = simd_sub_f64(&a, &b);
        assert_eq!(result, vec![9.0, 18.0, 27.0, 36.0, 45.0, 54.0, 63.0, 72.0, 81.0]);
    }

    #[test]
    fn test_simd_mul_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let result = simd_mul_f64(&a, &b);
        assert_eq!(result, vec![2.0, 6.0, 12.0, 20.0, 30.0, 42.0, 56.0, 72.0, 90.0]);
    }

    #[test]
    fn test_simd_div_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let b = vec![2.0, 4.0, 3.0, 8.0, 5.0, 6.0, 7.0, 10.0, 9.0];
        let result = simd_div_f64(&a, &b);
        assert_eq!(result, vec![5.0, 5.0, 10.0, 5.0, 10.0, 10.0, 10.0, 8.0, 10.0]);
    }

    #[test]
    fn test_simd_add_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![10, 20, 30, 40, 50, 60, 70, 80, 90];
        let result = simd_add_i64(&a, &b);
        assert_eq!(result, vec![11, 22, 33, 44, 55, 66, 77, 88, 99]);
    }

    #[test]
    fn test_simd_mul_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = simd_mul_i64(&a, &b);
        assert_eq!(result, vec![2, 6, 12, 20, 30, 42, 56, 72, 90]);
    }
}
