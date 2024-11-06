#####################
# FluxModelJ0408.py #
#####################


# This file contains a flux model for J0408-6545 .
# The model is converted to another model, using a fit at L-band.
# The parameters of the new model are printed, and can be used with the CASA setjy task.
# Code is taken from:
# https://skaafrica.atlassian.net/wiki/spaces/ESDKB/pages/1481408634/Flux+and+bandpass+calibration#J0408-6545
# Adapted by Joris Kersten (2024-11-06).
# The input model data specific to J0408-6545 is near the bottom. Apparently it is from 2016.


# Imports.
import numpy as np


def casa_flux_model(lnunu0, iref, *args):
    """
    Compute model:
    iref * 10**lnunu0 ** (args[0] + args[1] * lnunu0 + args[1] * lnunu0 ** 2 + args[0] * lnunu0 ** 3)
    """
    exponent = np.sum([arg * (lnunu0 ** (power))
                       for power, arg in enumerate(args)], axis=0)
    return iref * (10 ** lnunu0) ** (exponent)


def fit_flux_model(nu, s, sigma, nu0, sref=1, order=5):
    from scipy.optimize import curve_fit
    # from scipy.special import binom
    """
    From flux values at given frequencies and their uncertainties,
    fit an ASA style flux model specified by (reffreq,fluxdensity,spix[0],spix[1],spix[2],..) in the form
    S/Jy = (fluxdensity/Jy) * (freq/reffreq)**( spix[0] + spix[1]*log10(freq/reffreq) + spix[2]*log10(freq/reffreq)**2 + .. )
    Sometimes. but very rarely, the requested fit fails, in which case we fall back to a lower order,
    iterating until zeroth order. If all else fails return the weighted mean of the components.
    Finally convert the fitted parameters to a katpoint FluxDensityModel:
    log10(S) = a + b*log10(nu) + c*log10(nu)**2 + ...
    
    Parameters
    ----------
    nu : np.ndarray
        Frequencies to fit in Hz.
    s : np.ndarray
        Flux densities to fit in Jy.
    sigma : np.ndarray
        Errors of s.
    nu0 : float
        Reference frequency in Hz.
    sref : float (default 1
        Initial guess for the value of s at nu0
    order : int (optional)
        The desired order of the fitted flux model (1: SI, 2: SI + Curvature ...)
    
    Returns (with number of coefficients specified by order):
        [reffreq,fluxdensity,spix[0],spix[1],spix[2],..]
    """

    init = [sref, -0.7] + [0] * (order - 1)
    lnunu0 = np.log10(nu / nu0)
    for fitorder in range(order, -1, -1):
        try:
            popt, _ = curve_fit(casa_flux_model, lnunu0, s, p0=init[:fitorder + 1], sigma=sigma)
        except RuntimeError:
            print("Fitting flux model of order %d to CC failed. Trying lower order fit." %(fitorder,))
        else:
            coeffs = np.pad(popt, ((0, order - fitorder),), "constant")
            return [nu0] + coeffs.tolist()
    # Give up and return the weighted mean.
    coeffs = [np.average(s, weights=1. / (sigma ** 2))] + [0] * order
    return [nu0] + coeffs.tolist()


def convert_flux_model(nu=np.linspace(0.9, 2, 200) * 1e9, a=1, b=0, c=0, d=0, Reffreq=1.0e9):
    """
    Convert a flux model specified by (a,b,c,d) and from the form
    log10(S/Jy) = a + b*log10(nu/MHz) + c*log10(nu/MHz)**2 + ...
    to an ASA style flux model specified by (reffreq,fluxdensity,spix[0],spix[1],spix[2],..) in the form
    S/Jy = (fluxdensity/Jy) * (freq/reffreq)**( spix[0] + spix[1]*log10(freq/reffreq) + spix[2]*log10(freq/reffreq)**2 + .. )
    
    Parameters
    ----------
    nu : np.ndarray
        Frequencies to fit in Hz.
    a,b,c,d : float
        Parameter of a log flux model.
    Reffreq : float
        Reference frequency in Hz.
    
    Returns:
        [reffreq,fluxdensity,spix[0],spix[1],spix[2]]
    """
    MHz = 1e6
    S = 10 ** (a + b * np.log10(nu / MHz) + c * np.log10(nu / MHz) ** 2 + d * np.log10(nu / MHz) ** 3)
    return fit_flux_model(nu, S, np.ones_like(nu), Reffreq, sref=1, order=3)


# Here the flux model of J0408-6545 is defined. It is unclear to me (JK) from which measurements it is derived.
# The specification means: log10(S/Jy) = a + b*log10(nu/MHz) + c*log10(nu/MHz)**2 + d*log10(nu/MHz)**3 .
# So, S/Jy = 10^a * (nu/MHz)^b * 10^(c*log10^2(nu/MHz)) * 10^(d*log10^3(nu/MHz)) .
# name=0408-65 epoch=2016 ra=04h08m20.4s dec=-65d45m09s a=-0.9790 b=3.3662 c=-1.1216  
a = -0.9790
b = 3.3662
c = -1.1216
d = 0.0861

# L-band receiver edge frequencies in MHz, channel width for 1024 channels and central freq of channels 1 and 1024.
Llow_edge = 856.
Lhigh_edge = 1712.
Lchannels = 1024
Lwidth= (Lhigh_edge-Llow_edge)/Lchannels
Llow_centre = Llow_edge + Lwidth/2.
Lhigh_centre = Lhigh_edge - Lwidth/2.
Lcentre = Llow_edge + (Lhigh_edge - Llow_edge)/2.

# Note that the range for which the receiver is designed is 900 MHz - 1670 MHz.
# This means that for 1024 channels, channels 1-53 and 974-1024 are fully or partly outside this range.
# The good range is 54-973. So 920 out of 1024 channels.
# The central frequency of the good frequency range is not 1284 MHz, but 1285 MHz.
# The central frequency of the good channel range is 1284.8359375 MHz.
# To keep the central frequency at 1284 MHz one can remove two more channels at the high end,
# or add one at the low end and remove one at the high end, or add two at the low end.
# For the middle option the good channels are 53-972, which means edges 899.468750 MHz - 1668.531250 MHz.

# The linspace of frequencies in Hz for which to specify a flux and the central frequency in Hz.
Llinspace = np.linspace(Llow_centre, Lhigh_centre, Lchannels)*1e6
LcentreHz = Lcentre * 1e6

# We specify the flux at each channel's central frequency, and pass these values to the model converter.
# Every 'measurement' will get the same uncertainty, namely 1 Jy.
reffreq, fluxdensity, spix0, spix1, spix2 = convert_flux_model(Llinspace, a, b, c, d, LcentreHz)
# reffreq, fluxdensity, spix0, spix1, spix2 = convert_flux_model(Llinspace[53:972], a, b, c, d, LcentreHz)
# reffreq, fluxdensity, spix0, spix1, spix2 = convert_flux_model(Llinspace[52:971], a, b, c, d, LcentreHz)
f_cal_alt = 'J0408-6545'


print("Flux model converter")
print("--------------------")
print()
print("Input model:  S/Jy = 10**a * (nu/MHz)**b * 10**(c*log10^2(nu/MHz)) * 10**(d*log10^3(nu/MHz))")
print("Output model: S/Jy = "
      "(fluxdensity/Jy) * (freq/reffreq)**( spix[0] + spix[1]*log10(freq/reffreq) + spix[2]*log10(freq/reffreq)**2")
print()
print("Fit for source '{}':".format(f_cal_alt))
print("Input: a = -0.9790, b = 3.3662, c = -1.1216, d = 0.0861")
print()
print("Reference frequency (MHz):             {}".format(reffreq/1e6))
print("Flux density (Jy) at the ref. freq.:   {}".format(fluxdensity))
print("Model coefficient 1:                   {:+}".format(spix0))
print("Model coefficient 2:                   {:+}".format(spix1))
print("Model coefficient 3:                   {:+}".format(spix2))
print()
# print(reffreq, fluxdensity, spix0, spix1, spix2)


# setjy(vis=msfile,
#       field=f_cal_alt,
#       spix=[spix0, spix1, spix2, 0],
#       fluxdensity=fluxdensity,
#       reffreq='%f Hz' % (reffreq),
#       standard='manual')
